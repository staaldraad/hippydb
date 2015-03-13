var argv = require('minimist')(process.argv.slice(2));
var thrift = require('thrift');

var HiveBase = require('./gen-nodejs/ThriftHive');
var FacebookBase = require('./gen-nodejs/FacebookService');

var port = 10000 //9090
var host = argv.h
var connection = thrift.createConnection(host, port, { transport: thrift.TBufferedTransport, protocol:thrift.TBinaryProtocol, timeout:1000 });


connection.on('connect', function () {
    console.log('[*] Connected to: ['+host+']');
        var client = thrift.createClient(HiveBase, connection);
        client.get_all_databases(function(err,dbs){
                   console.log("[+] Databases: ",dbs)
                   dbs.forEach(function(db){
                       console.log("[+] Tables in ["+db+"]")
                       client.get_all_tables(db,function(err,tables){
                            console.log(tables)
                       })
                    })
                    connection.end()
        })
});

connection.on('error',function(err){
   console.log(err) 
   connection.end()

});
