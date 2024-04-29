const mongoose = require('mongoose');
const fs = require('fs');

const readline = require('readline');
const index = process.argv.indexOf('--fileTargeted');
const fileToDigest = process.argv[index+1]
 start();
async function start(){
    let startTime=new Date().getTime();
    console.log("Start")
    try {
        await mongoose.connect('mongodb://127.0.0.1:27017/etablissements');
        const fileStream = fs.createReadStream('splitted/'+fileToDigest);
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });
        let header=null;
        let mapHeaderIndex=null;

        for await (const line of rl) {
            if(header===null){
                header=line;
                mapHeaderIndex=headerToMapHeaderIndex(header);
            }else{
                await lineToObject(line,mapHeaderIndex).save()
                    .catch(reason => console.log(reason));
            }

        }

        await mongoose.disconnect();
        console.log("FINISH")
        console.log("With "+Math.round(new Date().getTime()-startTime)+"s")
        process.exit();
    }catch (e){
        console.log(e)
        process.exit();
    }




}








const Etablissements = mongoose.model('Etablissements', {
    siren:String,
    nic:String,
    siret:String,
    dateCreationEtablissement:String,
    dateDernierTraitementEtablissement:String,
    typeVoieEtablissement:String,
    libelleVoieEtablissement:String,
    codePostalEtablissement:String,
    libelleCommuneEtablissement:String,
    codeCommuneEtablissement:String,
    dateDebut:String,
    etatAdministratifEtablissement:String,
});



function lineToObject(line,mapHeaderIndex){
    const splitedLine= line.split(",")
    etablissement= new Etablissements({
        siren: splitedLine[mapHeaderIndex.siren],
        nic: splitedLine[mapHeaderIndex.nic],
        siret: splitedLine[mapHeaderIndex.siret],
        dateCreationEtablissement: splitedLine[mapHeaderIndex.dateCreationEtablissement],
        dateDernierTraitementEtablissement: splitedLine[mapHeaderIndex.dateDernierTraitementEtablissement],
        typeVoieEtablissement: splitedLine[mapHeaderIndex.typeVoieEtablissement],
        libelleVoieEtablissement: splitedLine[mapHeaderIndex.libelleVoieEtablissement],
        codePostalEtablissement: splitedLine[mapHeaderIndex.codePostalEtablissement],
        libelleCommuneEtablissement: splitedLine[mapHeaderIndex.libelleCommuneEtablissement],
        codeCommuneEtablissement: splitedLine[mapHeaderIndex.codeCommuneEtablissement],
        dateDebut: splitedLine[mapHeaderIndex.dateDebut],
        etatAdministratifEtablissement: splitedLine[mapHeaderIndex.etatAdministratifEtablissement],

    })
    for (const property in etablissement){
        if(etablissement[property]===""||etablissement[property]==null){
            delete etablissement[property] ;
        }
    }

    return etablissement;
}

function headerToMapHeaderIndex(header){
const splitedHeader= header.split(",")


    return {
        siren:splitedHeader.indexOf("siren"),
        nic:splitedHeader.indexOf("nic"),
        siret:splitedHeader.indexOf("siret"),
        dateCreationEtablissement:splitedHeader.indexOf("dateCreationEtablissement"),
        dateDernierTraitementEtablissement:splitedHeader.indexOf("dateDernierTraitementEtablissement"),
        typeVoieEtablissement:splitedHeader.indexOf("typeVoieEtablissement"),
        libelleVoieEtablissement:splitedHeader.indexOf("libelleVoieEtablissement"),
        codePostalEtablissement:splitedHeader.indexOf("codePostalEtablissement"),
        libelleCommuneEtablissement:splitedHeader.indexOf("libelleCommuneEtablissement"),
        codeCommuneEtablissement:splitedHeader.indexOf("codeCommuneEtablissement"),
        dateDebut:splitedHeader.indexOf("dateDebut"),
        etatAdministratifEtablissement:splitedHeader.indexOf("etatAdministratifEtablissement"),
    }
}