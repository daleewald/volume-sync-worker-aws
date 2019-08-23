const redis = require('redis');
const Queue = require('bee-queue');
const AWSUtility = require('./aws-utility');

const REGION = process.env.REGION;

console.log('Setup Queue');
const eq = new Queue('AWS-INVENTORY-EVENTS', {
    redis: {
        host: 'redis'
    }
});

const rclient = redis.createClient( { host: 'redis' });

eq.on('ready', () => {
    console.log('AWS Queue worker ready');

    eq.process((job, done) => {
        const evt = job.data.event;
        console.log('Handling', evt);
        
        if (evt === 'addDir' || evt === 'removeDir') {
            done( null, 'Nothing to do.');
        } else {
            const bucketName = job.data.targetBucket;
            const awsUtility = new AWSUtility( REGION, bucketName );

            if (evt === 'inventory') {
                awsUtility.inventory(job.data.projection).then( ( result ) => {
                    const cacheKey = [bucketName,'inventory'].join('::');
                    console.log('Cache inventory; key =', cacheKey);
                    rclient.set(cacheKey, JSON.stringify( result ), ( err, reply ) => {
                        if ( err ) {
                            console.log('ERROR', err);
                        } else {
                            console.log( reply );
                            done( null, cacheKey );
                        }
                    });
                    
                }).catch( err => done( err ));
            } else {
                const sourcePath = job.data.sourceFileName;
                const targetPath = job.data.targetFileName;
                if (evt === 'update') {
                    awsUtility.upload( sourcePath, targetPath ).then( ( result ) => {
                        done( null, result );
                    }).catch( ( err ) => done( err ));
                } else
                if (evt === 'remove') {
                    awsUtility.remove( targetPath ).then( ( result ) => {
                        done( null, result );
                    }).catch( err => done( err ));
                }
            }
        }
    });
});

eq.on('error', (err) => {
    console.log('Queue error: ', err.message);
});