const redis = require('redis');
const Queue = require('bee-queue');
const logger = require('./logger');
const AWSUtility = require('./aws-utility');

const REGION = process.env.REGION;

let queueName = 'AWS-INVENTORY-EVENTS';
logger.info('Open Queue', queueName);
const eq = new Queue(queueName, {
    redis: {
        host: 'redis'
    }
});

// redis client for general cache support
const rclient = redis.createClient( { host: 'redis' });


const subClient = rclient.duplicate();
const CHNL_CONTAINER_LOG_LEVEL = 'CONTAINER-LOG-LEVEL';

subClient.on("message", ( channel, message ) => {
    switch (channel) {
        case CHNL_CONTAINER_LOG_LEVEL:
            // should have been validated before being queued
            logger.info('',channel,'received message',message);
            logger.level = message;
            break;
        default:
    }
});
subClient.subscribe(CHNL_CONTAINER_LOG_LEVEL);


eq.on('ready', () => {
    logger.info('AWS Queue Worker ready');

    eq.process((job, done) => {
        const evt = job.data.event;
        logger.debug('Handling', evt);
        
        if (evt === 'addDir' || evt === 'removeDir') {
            done( null, 'Nothing to do.');
        } else {
            const bucketName = job.data.targetBucket;
            const awsUtility = new AWSUtility( REGION, bucketName );

            if (evt === 'inventory') {
                awsUtility.inventory(job.data.projection).then( ( result ) => {
                    const cacheKey = [queueName, bucketName,'inventory'].join(':');
                    logger.info('Cache inventory; key =', cacheKey);
                    rclient.set(cacheKey, JSON.stringify( result ), ( err, reply ) => {
                        if ( err ) {
                            logger.error('ERROR', err);
                        } else {
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
    logger.error(queueName,' error: ', err.message);
});