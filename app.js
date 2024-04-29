const pm2 = require('pm2')
const fs = require('fs');

try{
    pm2.connect(function (err) {
        const fileToSplit="StockEtablissement_utf8.csv"
        if (err) {
            console.error(err)
            process.exit(2)
        }



        setBus();

        fs.readdir("./splitted", (err, files) => {
            if(err){
                console.log(err)
            }
            if(files.length===0){
                startSplitter(fileToSplit);
            }else{
                startDigesterOrchestrator();
            }
        });
    })
}catch (e){
    console.log(e)
}



function setBus() {
    pm2.launchBus((err, bus) => {
        bus.on('process:splitFinish', (packet) => {
            console.log("[STEP] SPLIT : FINISH");
             pm2.stop('splitter');
        })
        bus.on('process:digesterOrchestratorFinish', (packet) => {
            console.log("[STEP] DIGESTER_ORCHESTRATOR : FINISH");
            pm2.stop('DigesterOrchestra');
        })

        bus.on('process:splitEmit', (packet) => {
            console.log(packet.data.value)
            startDigester(packet.data.value)
        })
    })
}

function startSplitter(fileToSplit) {
    console.log("[STEP] SPLIT : START");
    const name = "splitter";
    pm2.start({
        script: 'splitter.js',
        name: name,
        args:"--fileToSplit "+fileToSplit,
        autorestart: false,
        out_file: "./log/logSplitter"
    }, function (err, apps) {
        if (err) {
            console.log("[STEP] SPLIT : ERROR");
            console.error(err)
            return pm2.disconnect()
        }


    })

}


function startDigesterOrchestrator() {

    console.log("[STEP] DIGESTER_ORCHESTRATOR : START");
    const name = "DigesterOrchestra";
    pm2.start({
        script: 'digesterOrchestrator.js',
        name: name,
        autorestart: false,
        out_file: "./log/logDigesterOrchestrator"
    }, function (err, apps) {
        if (err) {
            console.log("[STEP] DIGESTER_ORCHESTRATOR : ERROR");
            console.error(err)
            return pm2.disconnect()
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