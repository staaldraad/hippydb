var thrift = require('thrift');
var util = require('util')
var HBase = require('./gen-nodejs/Hbase');
var HBaseTypes = require('./gen-nodejs/Hbase_types');
var port = 9090
 
//var connection = thrift.createConnection(process.argv[2], port, { transport: thrift.TFramedTransport,protocol:thrift.TBinaryProtocol });
var connection = thrift.createConnection(process.argv[2], port, { transport: thrift.TBufferedTransport, protocol:thrift.TBinaryProtocol });
 
connection.on('connect', function () {
    console.log('[*] Connected: '+process.argv[2]);
 
    var client = thrift.createClient(HBase, connection);
        client.getTableNames(function(err,data) {
        if (err) {
              console.log('gettablenames error:', err);
        } else {
              console.log('hbase tables:');
                      data.forEach(function(table){
                  console.log('[*] ',table.toString())
                      });
        }
        connection.end();
    });
});
connection.on('error',function(err){
     console.error("[x] Ay-ya-ya: "+err)
})
