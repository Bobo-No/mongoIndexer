const fs = require('fs');
const pm2 = require('pm2')
const readline = require('readline');
const instanceArgIndex = process.argv.indexOf('--fileToSplit');
const fileTargeted= process.argv[instanceArgIndex+1];

splitFile(fileTargeted);
function getWriteStream(fileName){
  return   fs.createWriteStream(fileName, {
        flags: 'a'
    })
}


async function splitFile(fileName) {
    const fileStream = fs.createReadStream('input/'+fileName);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });
    let counter=9999999999999999999999999999999;
    let fileCounter =0;
    let header=null;
    let writeStream;
    for await (const line of rl) {
        if(header===null){
            header=line;
        }
        counter++;
        if(counter>1000000){


            if(writeStream){
                writeStream.end();
                process.send({
                    type: 'process:splitEmit',
                    data: {
                        value:fileCounter,
                        success: true
                    }
                })
            }
            fileCounter++
            counter=0;
            writeStream=getWriteStream("splitted/splitted-"+fileCounter+".csv");

            writeStream.write(header+'\n')
        }
        writeStream.write(line+'\n')
    }
writeStream.end()
    process.send({
        type: 'process:splitFinish',
        data: {
            success: true
        }
    })
}


