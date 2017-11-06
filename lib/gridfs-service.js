var util = require('util');
var url = require('url');

var _ = require('lodash');
var Busboy = require('busboy');
var GridFS = require('gridfs-stream');
var ZipStream = require('zip-stream');
var stream_node = require('stream');

var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var crypto = require('crypto');
var ar = require('ar');
var tar = require('tar-stream');
var streamToBuffer = require('stream-to-buffer');
var gunzip = require('gunzip-maybe');
var sbuff = require('simple-bufferstream');
var ControlParser = require('debian-control-parser');
var fs = require('fs');
var streamBuffers = require('stream-buffers');
var concat = require('concat-stream');
var multiparty = require('multiparty');
module.exports = GridFSService;

function GridFSService(options) {
  if (!(this instanceof GridFSService)) {
    return new GridFSService(options);
  }

  this.options = options;
}

/**
 * Connect to mongodb if necessary.
 */
GridFSService.prototype.connect = function (cb) {
  var self = this;

  if (!this.db) {
    var url;
    if (!self.options.url) {
      url = (self.options.username && self.options.password) ?
        'mongodb://{$username}:{$password}@{$host}:{$port}/{$database}' :
        'mongodb://{$host}:{$port}/{$database}';

      // replace variables
      url = url.replace(/\{\$([a-zA-Z0-9]+)\}/g, function (pattern, option) {
        return self.options[option] || pattern;
      });
    } else {
      url = self.options.url;
    }

    // connect
    MongoClient.connect(url, self.options, function (error, db) {
      if (!error) {
        self.db = db;
      }
      return cb(error, db);
    });
  }
};

/**
 * List all storage containers.
 */
GridFSService.prototype.getContainers = function (cb) {
  var collection = this.db.collection('fs.files');

  collection.find({
    'metadata.container': {$exists: true}
  }).toArray(function (error, files) {
    var containerList = [];

    if (!error) {
      containerList = _(files)
        .map('metadata.container').uniq().value();
    }

    return cb(error, containerList);
  });
};


/**
 * Delete an existing storage container.
 */
GridFSService.prototype.deleteContainer = function (containerName, cb) {
  var fs_files = this.db.collection('fs.files');
  var fs_chunks = this.db.collection('fs.chunks');

  fs_files.find({'metadata.container': containerName}, {_id: 1}).toArray(function (error, containerFiles) {
    var files = [];
    for (var index in containerFiles) {
      files.push(containerFiles[index]._id);
    }

    fs_chunks.deleteMany({
      'files_id': {$in: files}
    }, function (error) {
      return cb(error);
    });

    fs_files.deleteMany({
      'metadata.container': containerName
    }, function (error) {
      return cb(error);
    });
  });
};


/**
 * List all files within the given container.
 */
GridFSService.prototype.getFiles = function (containerName, cb) {
  var collection = this.db.collection('fs.files');

  collection.find({
    'metadata.container': containerName
  }).toArray(function (error, container) {
    return cb(error, container);
  });
};


/**
 * Return a file with the given id within the given container.
 */
GridFSService.prototype.getFile = function (containerName, fileId, cb) {
  var collection = this.db.collection('fs.files');

  collection.find({
    '_id': new mongodb.ObjectID(fileId),
    'metadata.container': containerName
  }).limit(1).next(function (error, file) {
    if (!file) {
      error = new Error('File not found');
      error.status = 404;
    }
    return cb(error, file || {});
  });
};


/**
 * Delete an existing file with the given id within the given container.
 */
GridFSService.prototype.deleteFile = function (containerName, fileId, cb) {
  var fs_files = this.db.collection('fs.files');
  var fs_chunks = this.db.collection('fs.chunks');

  fs_files.deleteOne({
    '_id': new mongodb.ObjectID(fileId),
    'metadata.container': containerName
  }, function (error) {
    if (!error) {
      fs_chunks.deleteOne({
        'files_id': new mongodb.ObjectID(fileId)
      }, function (error) {
        cb(error);
      });
    } else {
      cb(error);
    }
  });
};


/**
 * Upload middleware for the HTTP request.
 */
GridFSService.prototype.upload = function (containerName, req, cb) {
  var self = this;

  var form = new multiparty.Form();
  var count = 0;


// Parts are emitted when parsing the form

  form.on('error', function(err) {
    console.log('Error parsing form: ' + err.stack);
  });

  form.on('part', function (part) {
    // You *must* act on the part by reading it
    // NOTE: if you want to ignore it, just call "part.resume()"

    if (!part.filename) {
      // filename is not defined when this is a field and not a file
      console.log('got field named ' + part.name);
      // ignore field's content
      part.resume();
    }

    if (part.filename) {

      var filename = part.filename;
      // filename is defined when this is a file
      count++;
      //console.log(part);
      console.log('got file named ' + part.name);
      var file = part.pipe(new stream_node.PassThrough());
     // var file12 = part.pipe(new stream_node.PassThrough());
      part = file.pipe(new stream_node.PassThrough());
     // var file1 = file.pipe(new stream_node.PassThrough());
      part.pipe(concat(function (bufferData) {
        var file12 = new stream_node.PassThrough();
        file12.end(bufferData);
        // file12._read = function () {} // _read is required but you can noop it
        // file12.push(bufferData);

        var archive = new ar.Archive(bufferData);
        var files = archive.getFiles();
        console.log(files);
        console.log(files.length);
        // part.resume();

        for (var i = 0; i < files.length; i++) {
          var file1 = files[i];
          console.log(file1);
          if (file1.name() == 'control.tar.gz') {
            var extract = tar.extract()

            extract.on('entry', function (header, stream, next) {
              console.log(header);
              var gotControl = 0;
              if (header.name.indexOf('control') !== -1 && gotControl == 0) {
                gotControl = 1;
                var control = ControlParser(stream);
                var gotStanza = 0;
                control.on('stanza', function (stanza) {
                  console.log('Got To Stanza');
                  //buffer = 0;
                  if (gotStanza == 0 && stanza['Name']) {
                    gotStanza = 1;
                    var sha1Stream = new stream_node.PassThrough();
                    sha1Stream.end(bufferData);
                   //. var sha1Stream = file.pipe(new stream_node.PassThrough());
                    sha1Stream.pipe(crypto.createHash('sha1').setEncoding('hex')).on('finish', function () {
                      var sha1 = this.read();
                      // stream1 = 0;

                      var sha256Stream = new stream_node.PassThrough();
                      sha256Stream.end(bufferData);
                      sha256Stream.pipe(crypto.createHash('sha256').setEncoding('hex')).on('finish', function () {
                        var sha256 = this.read();
                        //  stream2 = 0;
                        console.log('Got to this step');

                        var options = {
                          _id: new mongodb.ObjectID(),
                          filename: filename,
                          metadata: {
                            container: containerName,
                            filename: filename,
                            mimetype: 'application/vnd.debian.binary-package',
                            sha1: sha1,
                            sha256: sha256
                          },
                          mode: 'w'
                        };

                        // var sha256Stream1 = file.pipe(new stream_node.PassThrough());

                        var gridfs = new GridFS(self.db, mongodb);
                        var stream6 = gridfs.createWriteStream(options);

                        stream6.on('close', function (file2) {
                          return cb(null, file2, stanza);
                        });

                        stream6.on('error', cb);

                      //  var sha256Stream1 = part.pipe(new stream_node.PassThrough());

                        file12.pipe(stream6);
                      });
                    })
                  }
                  //  console.log(stanza);
                });
              } else {
                stream.on('end', function () {
                  next() // ready for next entry
                })
              }

              stream.resume()
            })

            sbuff(file1.fileData()).pipe(gunzip()).pipe(extract);
          }
        }
      }))
    }
  });
  form.parse(req); // Parse the upload request
}


// Close emitted after form parsed
// Parse req
//console.log(req);

//  var busboy = new Busboy({
//    headers: req.headers
//  });
//
// // console.log(req.headers);
//
//
//
// // console.log(req.headers.toString());
//
//  busboy.on('file', function (fieldname, file, filename, encoding, mimetype) {
//    console.log(mimetype);
//    console.log(fieldname);
//    file.pipe(concat(function (data) {
//      var archive = new ar.Archive(data);
//                var files = archive.getFiles();
//                console.log(files);
//    }));
//   // file.pipe(fs.createWriteStream('./shit3.deb'));
//
//    //
//    //
//    // const chunks = [];
//    //
//    // file.on("readable", function () {
//    //   var buf = file.read();
//    //   console.dir(buf);
//    // });
//    //
//    // // Send the buffer or you can put it into a var
//    // file.on("end", function () {
//    //   console.log(Buffer.concat(chunks));
//    // });
//
//    // file.on('data', function() {
//    //   console.log('got data')
//    // })
//   // var stream4 = file.pipe(new stream_node.PassThrough());
//   //  file.on('readable', function () {
//   //    file.pipe(fs.createWriteStream('./shit2.deb').on('finish', function () {
//   //      var readStream = fs.createReadStream('./shit2.deb');
//   //      readStream.on('readable', function() {
//   //        // var myWritableStreamBuffer = new streamBuffers.WritableStreamBuffer({
//   //        //   initialSize: (60 * 1024),   // start at 100 kilobytes.
//   //        //   incrementAmount: (10 * 1024) // grow by 10 kilobytes each time buffer overflows.
//   //        // });
//   //
//   //        //readStream.pipe(myWritableStreamBuffer).on('finish', function() {
//   //          var archive = new ar.Archive(fs.readFileSync('./shit.deb'));
//   //          var files = archive.getFiles();
//   //          console.log(files);
//   //       // })
//   //      })
//   //    }));
//
//
//      // var myWritableStreamBuffer = new streamBuffers.WritableStreamBuffer({
//      //   initialSize: (60 * 1024),   // start at 100 kilobytes.
//      //   incrementAmount: (10 * 1024) // grow by 10 kilobytes each time buffer overflows.
//      // });
//      //
//      //
//      // var thg = file.pipe(myWritableStreamBuffer);
//      // thg.on('finish', function() {
//      //
//      //   var archive = new ar.Archive(thg.getContents());
//      //   var files = archive.getFiles();
//      //
//      //   console.log(files);
//      //
//      //   for (var i = 0; i < files.length; i++) {
//      //     var file1 = files[i];
//      //     if (file1.name() == 'control.tar.gz') {
//      //       var extract = tar.extract()
//      //
//      //       extract.on('entry', function(header, stream, next) {
//      //
//      //         console.log(header);
//      //         var gotControl = 0;
//      //         if (header.name.indexOf('control') !== -1 && gotControl == 0) {
//      //           gotControl = 1;
//      //           var control = ControlParser(stream);
//      //           var gotStanza = 0;
//      //           control.on('stanza', function(stanza) {
//      //             console.log('Got To Stanza');
//      //             //buffer = 0;
//      //             if (gotStanza == 0 && stanza['Name']) {
//      //               gotStanza = 1;
//      //               var stream1 = file.pipe(new stream_node.PassThrough());
//      //               stream1.pipe(crypto.createHash('sha1').setEncoding('hex')).on('finish', function () {
//      //                 var sha1 = this.read();
//      //                 // stream1 = 0;
//      //
//      //                 var stream2 = file.pipe(new stream_node.PassThrough());
//      //                 stream2.pipe(crypto.createHash('sha256').setEncoding('hex')).on('finish', function () {
//      //                   var sha256 = this.read();
//      //                   //  stream2 = 0;
//      //                   console.log('Got to this step');
//      //
//      //                   var options = {
//      //                     _id: new mongodb.ObjectID(),
//      //                     filename: filename,
//      //                     metadata: {
//      //                       container: containerName,
//      //                       filename: filename,
//      //                       mimetype: 'application/vnd.debian.binary-package',
//      //                       sha1: sha1,
//      //                       sha256: sha256
//      //                     },
//      //                     mode: 'w'
//      //                   };
//      //
//      //                   var gridfs = new GridFS(self.db, mongodb);
//      //                   var stream6 = gridfs.createWriteStream(options);
//      //
//      //                   stream6.on('close', function (file2) {
//      //                     return cb(null, file2, stanza);
//      //                   });
//      //
//      //                   stream6.on('error', cb);
//      //
//      //                   file.pipe(stream6);
//      //                 });
//      //               })
//      //             }
//      //             //  console.log(stanza);
//      //           });
//      //         } else {
//      //           stream.on('end', function() {
//      //             next() // ready for next entry
//      //           })
//      //         }
//      //
//      //         stream.resume()
//      //       })
//      //
//      //       sbuff(file1.fileData()).pipe(gunzip()).pipe(extract);
//      //     }
//      //     //fs.writeFileSync(path.resolve(outputDir, file.name()), file.fileData());
//      //   }
//      //   console.log(thg.getContents().toString());
//      // })
//
//      // console.log('first pipe done');
//      // var stream3 = file.pipe(new stream_node.PassThrough());
//      // stream3.on('readable', function () {
//      //   // var writeStream = fs.createWriteStream('./t');
//      //   // writeStream.on('')
//      //   // var filePipe = stream3.pipe(writeStream);
//      //   var stream33 = file.pipe(new stream_node.PassThrough());
//      //   stream33.on('readable', function () {
//      //    // console.log(stream4.read(33));
//      //     var data = stream4.read(4096);
//      //     console.log(data.toString('ascii'));
//      //     var archive = new ar.Archive(data);
//      //     var files = archive.getFiles();
//      //
//      //     console.log(files);
//      //
//      //     for (var i = 0; i < files.length; i++) {
//      //       var file1 = files[i];
//      //       if (file1.name() == 'control.tar.gz') {
//      //         var extract = tar.extract()
//      //
//      //         extract.on('entry', function(header, stream, next) {
//      //
//      //           console.log(header);
//      //           var gotControl = 0;
//      //           if (header.name.indexOf('control') !== -1 && gotControl == 0) {
//      //             gotControl = 1;
//      //             var control = ControlParser(stream);
//      //             var gotStanza = 0;
//      //             control.on('stanza', function(stanza) {
//      //               console.log('Got To Stanza');
//      //               //buffer = 0;
//      //               if (gotStanza == 0 && stanza['Name']) {
//      //                 gotStanza = 1;
//      //                 var stream1 = file.pipe(new stream_node.PassThrough());
//      //                 stream1.pipe(crypto.createHash('sha1').setEncoding('hex')).on('finish', function () {
//      //                   var sha1 = this.read();
//      //                   // stream1 = 0;
//      //
//      //                   var stream2 = file.pipe(new stream_node.PassThrough());
//      //                   stream2.pipe(crypto.createHash('sha256').setEncoding('hex')).on('finish', function () {
//      //                     var sha256 = this.read();
//      //                     //  stream2 = 0;
//      //                     console.log('Got to this step');
//      //
//      //                     var options = {
//      //                       _id: new mongodb.ObjectID(),
//      //                       filename: filename,
//      //                       metadata: {
//      //                         container: containerName,
//      //                         filename: filename,
//      //                         mimetype: 'application/vnd.debian.binary-package',
//      //                         sha1: sha1,
//      //                         sha256: sha256
//      //                       },
//      //                       mode: 'w'
//      //                     };
//      //
//      //                     var gridfs = new GridFS(self.db, mongodb);
//      //                     var stream6 = gridfs.createWriteStream(options);
//      //
//      //                     stream6.on('close', function (file2) {
//      //                       return cb(null, file2, stanza);
//      //                     });
//      //
//      //                     stream6.on('error', cb);
//      //
//      //                     file.pipe(stream6);
//      //                   });
//      //                 })
//      //               }
//      //               //  console.log(stanza);
//      //             });
//      //           } else {
//      //             stream.on('end', function() {
//      //               next() // ready for next entry
//      //             })
//      //           }
//      //
//      //           stream.resume()
//      //         })
//      //
//      //         sbuff(file1.fileData()).pipe(gunzip()).pipe(extract);
//      //       }
//      //       //fs.writeFileSync(path.resolve(outputDir, file.name()), file.fileData());
//      //     }
//      //   });
//      // });
//    })
//    //
//    // console.log(stream4);
//    // console.log(file);
//   // file.pipe(console.log)
//
//      // stream1.pipe(crypto.createHash('sha1').setEncoding('hex')).on('finish', function () {
//      //   var sha1 = this.read();
//      //   stream2.pipe(crypto.createHash('sha256').setEncoding('hex')).on('finish', function () {
//      //     var sha256 = this.read();
//      //
//      //     var options = {
//      //       _id: new mongodb.ObjectID(),
//      //       filename: filename,
//      //       metadata: {
//      //         container: containerName,
//      //         filename: filename,
//      //         mimetype: mimetype,
//      //         sha1: sha1,
//      //         sha256: sha256
//      //       },
//      //       mode: 'w'
//      //     };
//      //
//      //     var gridfs = new GridFS(self.db, mongodb);
//      //     var stream = gridfs.createWriteStream(options);
//      //
//      //     stream.on('close', function (file) {
//      //       return cb(null, file);
//      //     });
//      //
//      //     stream.on('error', cb);
//      //
//      //     stream3.pipe(stream);
//      //   });
//      // })
// // });
//
//  req.pipe(busboy);


/**
 * Download middleware for the HTTP request.
 */
GridFSService.prototype.download = function (containerName, fileId, res, cb) {
  var self = this;

  var collection = this.db.collection('fs.files');

  collection.find({
    '_id': new mongodb.ObjectID(fileId),
    'metadata.container': containerName
  }).limit(1).next(function (error, file) {
    if (!file) {
      error = new Error('File not found.');
      error.status = 404;
    }

    if (error) {
      return cb(error);
    }

    var gridfs = new GridFS(self.db, mongodb);
    var stream = gridfs.createReadStream({
      _id: file._id
    });

    // set headers
    res.set('Content-Type', file.metadata.mimetype);
    res.set('Content-Length', file.length);
    res.set('Content-disposition', 'attachment; filename=' + file.filename);

    return stream.pipe(res);
  });
};

GridFSService.prototype.downloadContainer = function (containerName, req, res, cb) {
  var self = this;

  var collection = this.db.collection('fs.files');

  collection.find({
    'metadata.container': containerName
  }).toArray(function (error, files) {
    if (files.length === 0) {
      error = new Error('No files to archive.');
      error.status = 404;
    }

    if (error) {
      return cb(error);
    }

    var gridfs = new GridFS(self.db, mongodb);
    var archive = new ZipStream();
    var archiveSize = 0;

    function next() {
      if (files.length > 0) {
        var file = files.pop();
        var fileStream = gridfs.createReadStream({_id: file._id});

        archive.entry(fileStream, {name: file.filename}, next);
      } else {
        archive.finish();
      }
    }

    next();

    var filename = req.query.filename || 'file';

    res.set('Content-Disposition', `attachment;filename=${filename}.zip`);
    res.set('Content-Type', 'application/zip');

    return archive.pipe(res);
  });
};


GridFSService.modelName = 'storage';

/*
 * Routing options
 */

/*
 * GET /FileContainers
 */
GridFSService.prototype.getContainers.shared = true;
GridFSService.prototype.getContainers.accepts = [];
GridFSService.prototype.getContainers.returns = {
  arg: 'containers',
  type: 'array',
  root: true
};
GridFSService.prototype.getContainers.http = {
  verb: 'get',
  path: '/'
};

/*
 * DELETE /FileContainers/:containerName
 */
GridFSService.prototype.deleteContainer.shared = true;
GridFSService.prototype.deleteContainer.accepts = [
  {arg: 'containerName', type: 'string', description: 'Container name'}
];
GridFSService.prototype.deleteContainer.returns = {};
GridFSService.prototype.deleteContainer.http = {
  verb: 'delete',
  path: '/:containerName'
};

/*
 * GET /FileContainers/:containerName/files
 */
GridFSService.prototype.getFiles.shared = true;
GridFSService.prototype.getFiles.accepts = [
  {arg: 'containerName', type: 'string', description: 'Container name'}
];
GridFSService.prototype.getFiles.returns = {
  type: 'array',
  root: true
};
GridFSService.prototype.getFiles.http = {
  verb: 'get',
  path: '/:containerName/files'
};

/*
 * GET /FileContainers/:containerName/files/:fileId
 */
GridFSService.prototype.getFile.shared = true;
GridFSService.prototype.getFile.accepts = [
  {arg: 'containerName', type: 'string', description: 'Container name'},
  {arg: 'fileId', type: 'string', description: 'File id'}
];
GridFSService.prototype.getFile.returns = {
  type: 'object',
  root: true
};
GridFSService.prototype.getFile.http = {
  verb: 'get',
  path: '/:containerName/files/:fileId'
};

/*
 * DELETE /FileContainers/:containerName/files/:fileId
 */
GridFSService.prototype.deleteFile.shared = true;
GridFSService.prototype.deleteFile.accepts = [
  {arg: 'containerName', type: 'string', description: 'Container name'},
  {arg: 'fileId', type: 'string', description: 'File id'}
];
GridFSService.prototype.deleteFile.returns = {};
GridFSService.prototype.deleteFile.http = {
  verb: 'delete',
  path: '/:containerName/files/:fileId'
};

/*
 * DELETE /FileContainers/:containerName/upload
 */
GridFSService.prototype.upload.shared = true;
GridFSService.prototype.upload.accepts = [
  {arg: 'containerName', type: 'string', description: 'Container name'},
  {arg: 'req', type: 'object', http: {source: 'req'}}
];
GridFSService.prototype.upload.returns = {
  arg: 'file',
  type: 'object',
  root: true
};
GridFSService.prototype.upload.http = {
  verb: 'post',
  path: '/:containerName/upload'
};

/*
 * GET /FileContainers/:containerName/download/:fileId
 */
GridFSService.prototype.download.shared = true;
GridFSService.prototype.download.accepts = [
  {arg: 'containerName', type: 'string', description: 'Container name'},
  {arg: 'fileId', type: 'string', description: 'File id'},
  {arg: 'res', type: 'object', 'http': {source: 'res'}}
];
GridFSService.prototype.download.http = {
  verb: 'get',
  path: '/:containerName/download/:fileId'
};

/*
 * GET /FileContainers/:containerName/download/zip
 */
GridFSService.prototype.downloadContainer.shared = true;
GridFSService.prototype.downloadContainer.accepts = [
  {arg: 'containerName', type: 'string', description: 'Container name'},
  {arg: 'req', type: 'object', 'http': {source: 'req'}},
  {arg: 'res', type: 'object', 'http': {source: 'res'}}
];
GridFSService.prototype.downloadContainer.http = {
  verb: 'get',
  path: '/:containerName/zip'
};
