const pm2 = require('pm2');
const dir = './splitted';
const fs = require('fs');
 start();

async function start() {
    let numberOfFileToDigest = 0;
     fs.readdir(dir, (err, files) => {
        numberOfFileToDigest = files.length;
        console.log(numberOfFileToDigest)
        for (let i = 1; i <= numberOfFileToDigest; i++) {
            startDigester(i);
        }
    });
    process.send({
        type: 'process:digesterOrchestratorFinish',
        data: {
            value:numberOfFileToDigest,
            success: true
        }
    })
}




function startDigester(instance) {
    const name = "digester";
    console.log("[STEP] DIGESTER ("+instance+") : START");
    pm2.start({
        script: 'digester.js',
        name: name,
        args: '--fileTargeted ' + instanceToFilename(instance),
        autorestart: false,
        out_file: "./log/digester-"+ instance
    }, function (err, apps) {
        if (err) {
            console.log("[STEP] DIGESTER ("+instance+") : ERROR");
            console.error(err)
            return pm2.disconnect()
        }
        console.log("[STEP] DIGESTER ("+instance+") : FINISH");

    })
}


function instanceToFilename(instance) {
    return "splitted-" + instance + ".csv";
}