var async = require('async');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var PDFDocument = require('pdfkit');
var doc = new PDFDocument({
  autoFirstPage: false
  });
var fs = require('fs');
var util = require('util');
var path = require('path');

exports.handler = function(event, context, callback) {

  // Read options from the event.
  console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));
  // var srcBucket = event.Records[0].s3.bucket.name;
  var srcBucket = 'chelajs';
  // var srcPrefix = event.Records[0].s3.object.key.replace(/\+/g, " ");
  var srcPrefix = 'process/';

  var tmpFolder = '/tmp';
  var stream = doc.pipe(fs.createWriteStream( path.join(tmpFolder,'/file.pdf') ));

  function error(err){
    if (err) {
      console.error(err);
    } else {
      console.log('Success!');
    }

    callback(null, "message");
  };

  function list(next) {
    s3.listObjectsV2({
        Bucket: srcBucket,
        Prefix: srcPrefix
    }, next);
  };

  function download(data, next){
    var keys = data.Contents;
    var keyLen = srcPrefix.length;

    var fileKeys = keys.filter( (file) => {
      return file.Key.length > keyLen
    }).map( (file) => file.Key );

    var streams = fileKeys.map( (fileKey) => {
      var fileName = path.join(tmpFolder, path.basename(fileKey));
      var file = fs.createWriteStream(fileName);

      s3.getObject({
        Bucket: srcBucket,
        Key: fileKey
      }).createReadStream().pipe(file);

      return function(callback){
        file.on('finish', function(err, data){
          if(err)
            callback(err);
          callback(null, fileName);
        });
      }
    });

    async.parallel(streams, next);
  }

  function appendToPdf(data, next) {

    if(!data.length){
      doc.end();
      console.log('no documents found');
      stream.on('finish', next);      
    }

    data.forEach( (stream) => {
      doc.addPage()
        .image(stream, 50, 50, { width: 300 });
    });

    doc.flushPages();
    doc.end();
    stream.on('finish', next);
  }

  function upload(next){
    var body = fs.createReadStream(stream.path);
    s3.upload({
      Bucket: srcBucket,
      Key: "result.pdf",
      Body: body,
      ContentType: 'application/pdf'
    }, next);
  }

  async.waterfall([list, download, appendToPdf, upload], error);
};
