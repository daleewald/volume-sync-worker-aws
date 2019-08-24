const AWSUtility = require('./aws-utility');

const awsUtility = new AWSUtility('us-east-1', 'test-data-sync-dewaldops');

awsUtility.inventory(['name','updated']).then( ( inv ) => {
    inv.forEach(file => {
        console.log(file);
    });
}).catch( err => { console.error( err ); });

awsUtility.upload( 'package.json', 'test/package.json').then( (result) => {
    console.log(result);
    awsUtility.remove( 'test/package.json' ).then( (result) => {
        console.log(result);
    }).catch( ( err ) => { console.log(err); });
}).catch( ( err ) => { console.log(err); });