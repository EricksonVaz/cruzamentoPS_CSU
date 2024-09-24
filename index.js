var parser = new (require('simple-excel-to-json').XlsParser)();
console.log("start load excel...")
var ListagemGeralPS = parser.parseXls2Json('./ListagemGeralPSJunho2024.xlsx');
var ListagemGeralMembrosCSU = parser.parseXls2Json('./ListagemGeralMembrosCSU_24092024.xlsx');
var json2xls = require('json2xls');
var stringSimilarity = require("string-similarity");
var fs = require("fs");
console.log("finish load excel...")

console.log("start iteration in excel...")
let arrListBIPS = ListagemGeralPS[0].map(el=>((el?.BI+"")??"").trim());
let arrCSUMembersFindByDOC = ListagemGeralMembrosCSU[0].filter(el=>arrListBIPS.includes(el.DocID+""))

console.log("finish iteration in excel...")
// console.log("Nome",arrCSUMembersFindByDOC)

console.log("start loop")
let totalNamesPS = ListagemGeralPS[0].length;
let currentTotal = 0
let objResultFindArr = []
let i = 0
for (const {N, BI, Nome, Residencia, Concelho} of ListagemGeralPS[0]) {
    i++;
    let csuOBJArr = arrCSUMembersFindByDOC.filter(el=>((el?.DocID+"")??"").trim()==(BI+"").trim());
    if(csuOBJArr.length){
        let listaNomesArr = csuOBJArr.map(el=>(el?.NomeCompleto+""??"").trim());
        let nomeToSearch = (Nome??"").trim();

        var matches = stringSimilarity.findBestMatch(nomeToSearch, listaNomesArr);
        let nameFinded = (matches.bestMatch.target ?? "").trim();
        let objCSUFind = csuOBJArr.find(el=>(el?.NomeCompleto+""??"").trim()==nameFinded);
        objResultFindArr.push({
            N:i,
            OBJ:"Encontrado",
            BI,
            Nome,
            Residencia,
            Concelho,
            ...objCSUFind
        });
    }else{
        objResultFindArr.push({
            N:i,
            OBJ:"Não Encontrado",
            BI,
            Nome,
            Residencia,
            Concelho,
            NIM:"",
            NomeCompleto:"",
            DocID:""
        });
    }

    currentTotal++;

    console.log("% completo",((currentTotal/totalNamesPS)*100))
}
console.log("finish loop")

console.log("start create excel")

var xlsToLoad = json2xls(objResultFindArr);

fs.writeFileSync(`./cruzamento_PS_CNCSU@PSJunho2024.xlsx`, xlsToLoad, 'binary');

console.log("finish create excel")