/*
   NodeJS interface to memcache. Allows for extracting keys and specific values.
   Author: etienne@sensepost.com
   Version: 02/02/2015 v0.1
*/
var host = process.argv[2]
var port = 11211

if(!process.argv[3])
{
   console.error('Usage: nodejs memcache_dump.js <host> [keys|slabs|values [number]]');
   return;
}

console.log('Checking host: '+host+':'+port)

var Memcached = require('memcached');
var memcached = new Memcached(host+':'+port);

var max = 10 //default max values to fetch
if(process.argv[3]=='values' && process.argv[4])
{ 
    max = parseInt(process.argv[4],10)
}
if(process.argv[3]=='keys'||process.argv[3]=='slabs'||process.argv[3]=='values')
{
    memcached.items(function(err, data){
            if(err) console.error( err );  

            data.forEach(function( itemSet ){
                 var keys = Object.keys(itemSet);
                 keys.pop()

                 if(process.argv[3]=='keys') console.log('-- Extracting Keys --')
                 else if(process.argv[3]=='slabs')console.log('-- Extracting slabs --')
                 else if(process.argv[3]=='values')console.log('-- Extracting Values ('+max+') --')

                 keys.forEach(function(stats){
                  if(process.argv[3]=='keys' || process.argv[3]=='values') //extract keys
                  {
                       memcached.cachedump(itemSet.server, parseInt(stats,10), itemSet[stats].number,function(err,result){                        
                             try{
                                 if(result&&result.length){
                                   result.forEach(function(res)
                                   {
                                       if(process.argv[3]=='values' && max>0){
                                           max--;
                                           memcached.get(res.key,function(err,data){
                                               console.info('Key: '+res.key+' Value: '+data)
                                           })
                                       }
                                       else if(process.argv[3]=='keys')
                                           console.info(res.key)
                                   })
                                 }
                                 else if(result)
                                 {
                                    if(process.argv[3]=='values' && max>0){
                                       max--;
                                       memcached.get(result.key,function(err,data){
                                           console.info('Key: '+result.key+' Value: '+data)
                                       })
                                     }
                                     else if(process.argv[3]=='keys')
                                          console.info(result.key)
                                   }
                               }catch (ex){}
                       })
                  }
                  else if(process.argv[3]=='slabs'){
                      console.info(stats,itemSet[stats].number)
                  }
                  
                 })
       })
    });//memcached.items
}
