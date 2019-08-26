const AWS = require('aws-sdk');
const fs = require('fs');
const logger = require('./logger');

class AWSUtility {
    s3;
    bucketName;

    fileProjection = {
        name: 'Key',
        updated: 'LastModified'
    }

    constructor( region, bucketName ) {
        this.setup(region, bucketName);
    }

    setup( region, bucketName ) {
        AWS.config.update({region: region});
        AWS.config.setPromisesDependency(Promise);

        this.s3 = new AWS.S3({apiVersion: '2006-03-01'});
        if (bucketName) {
            this.bucketName = bucketName;
        }
    }

    async upload( sourcepath, targetpath, params ) {
        logger.debug('Beginning upload:', targetpath, params);
        let uploadParams = params || {};
        const file = fs.createReadStream(sourcepath);
        if (uploadParams.Bucket === undefined) {
            uploadParams['Bucket'] = this.bucketName;
        }

        uploadParams['Key'] = targetpath;
        uploadParams['Body'] = file;
        
        let result;

        await this.s3.upload(uploadParams).promise().then( ( data ) => {
            result = {result: 'Uploaded', file: data.Key, etag: data.ETag};
        }).catch( ( err ) => {
            throw err;
        });
        return result;
    }

    async createBucket( bucketName, params ) {

    }

    async remove( targetPath, bucketName ) {
        const params = {
            Bucket: bucketName || this.bucketName, 
            Key: targetPath
        };
        let result;
        await this.s3.deleteObject(params).promise().then( ( data ) => {
            result = {result: 'Deleted', file: targetPath};
        }).catch( ( err ) => { throw err; });
        return result;
    }

    async inventory(projection, bucketName) {
        let params = {
            Bucket: bucketName || this.bucketName
        }
        let results = [];
        let done = false;
        let continuationToken = '';

        while (! done) {
            await this.s3.listObjectsV2( params ).promise().then( ( data ) => {
                
                if ( data && data.IsTruncated ) {
                    if (continuationToken === data.NextContinuationToken) {
                        // redundant since IsTruncated should be false here?
                        done = true;
                    } else {
                        continuationToken = data.NextContinuationToken;
                        params['ContinuationToken'] = continuationToken;
                    }
                } else {
                    done = true;
                }

                const resultSet = this.mapObjectList( data.Contents, projection );
                results = results.concat( resultSet );
            }).catch( ( err ) => { done = true; throw err; });
        }
        return results;
    }

    mapObjectList( objectList , projection) {
        return objectList.map( file => {
            let p = {};
            projection.forEach( key => {
                if (Array.isArray(this.fileProjection[key])) {
                    let t = file;
                    this.fileProjection[key].forEach( (v) => {
                        t = t[v];
                    });
                    p[key] = t;
                } else {
                    p[key] = file[this.fileProjection[key]];
                }
            });
            return p;
        });
    }
}

module.exports = AWSUtility;
