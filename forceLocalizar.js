var parser = new (require('simple-excel-to-json').XlsParser)();
console.log("start load excel...")
var ListagemGeralPS = parser.parseXls2Json('./cruzamento_PS_CNCSU.xlsx');
var stringSimilarity = require("string-similarity");
var json2xls = require('json2xls');
const sqlServer = require('mssql');

const csuBdConnection = {
    user:"CSUQCLogin",
    password: "@!Q5@2C*A&#",
    server: "csu.cv,1334",
    database: "CSU_PRD_GEO",
    encrypt:false,
    requestTimeout:3600000
}

async function getConnectionPool(connectioInfoOBJ){
    const {user,server,database,password,encrypt,requestTimeout} = connectioInfoOBJ;
    return await sqlServer.connect(`Server=${server};Database=${database};User Id=${user};Password=${password};Encrypt=${encrypt};request timeout=${requestTimeout}`)
}
var fs = require("fs");
console.log("finish load excel...");

console.log("start iteration in excel...")
let arrCSUFinded = ListagemGeralPS[0].filter(el=>(el?.OBJ??"").trim()=="Encontrado");
let arrCSUNotFinded = ListagemGeralPS[0].filter(el=>(el?.OBJ??"").trim()=="Não Encontrado");
console.log("finish iteration in excel...");

let listNIMArr = [...(new Set(arrCSUFinded.map(el=>(el?.NIM??"").trim())))];
let listConcelhoArr = [...(new Set(arrCSUNotFinded.map(el=>(el?.Concelho??"").trim())))].sort();

let nimString = listNIMArr.map(nim=>`'${nim}'`).join(",");

//console.log(listConcelhoArr);

async function getListMembersByConcelho(concelho){
    let querySQL = `
        SELECT 
            parentglobalid CSUGlobalID
            ,a.globalid MembroGlobalID
            ,b.IAF201 NIA
            ,[MAF3011] NIM
            ,b.L103 Concelho
            ,b.L107 Zona
            ,b.L108 ReferenciaMorada
            ,[MAF302] Membro
            ,MAF3142 DocID
            ,MAF315 NIF
            ,IAF203 AS Contacto
            ,a.EstadoMembro
            ,b.estadoficha CSUStatus
            ,b.GDB_TO_DATE UltimaAtualizacao
        FROM [CSU_PRD_GEO].[sde].[CSU_MEMBROS] a LEFT JOIN [CSU_PRD_GEO].[sde].[CSU_AGREGADOS] b
        ON a.parentglobalid = b.globalid
        WHERE a.GDB_TO_DATE = '9999-12-31 23:59:59.0000000'
        AND (
            b.GDB_TO_DATE = '9999-12-31 23:59:59.0000000' OR b.estadoficha in ('Anulado','Actualizado','Levantamento', 'Em espera')
        )
        AND b.L103 = '${concelho}'
        ORDER BY NIM, UltimaAtualizacao DESC
    `;

    let pool = await getConnectionPool(csuBdConnection);

    const result = await pool.request()
    .query(querySQL);

    pool.close();

    return result['recordset'] ?? [];
}

async function loadListaParaVerificar(){

    let listMembrosArr = []

    for (const concelho of listConcelhoArr) {
         console.log("get data from CSU concelho",concelho);
        let listMembros = await getListMembersByConcelho(concelho);

        listMembrosArr.push(...(listMembros.filter(el=>!(listNIMArr.includes(el.NIM)))))
    }

    console.log("finish load data from CSU");  

    let objResultFindArr = [];
    let i = 0

    totalNamesPS = arrCSUNotFinded.length;
    currentTotal = 0;

    // console.log("start load data from CSU");
    // listMembros = await getListMembersByConcelho();
    // console.log("finish load data from CSU");  

    for (const {BI,Nome,Residencia,Concelho} of arrCSUNotFinded) {
        i++;

        let listNames = listMembrosArr.filter(el=>(el?.Concelho??"").trim()==Concelho).map(el=>(el?.Membro??"").trim());

        let matches = stringSimilarity.findBestMatch((Nome??"").trim(), listNames);
        let rating = matches.bestMatch.rating;

        let nameFinded = (matches.bestMatch.target ?? "").trim();
        let objCSUFind = listMembrosArr.find(el=>(el?.Membro+""??"").trim()==nameFinded);

        objResultFindArr.push({
            N:i,
            OBJ:rating,
            BI,
            Nome,
            Residencia,
            Concelho,
            ...objCSUFind
        });

        currentTotal++;

        console.log("% completo",((currentTotal/totalNamesPS)*100))
    }

    console.log("start create excel")

    var xlsToLoad = json2xls(objResultFindArr);

    fs.writeFileSync(`./Lista_PS_CNCSU_Verificar.xlsx`, xlsToLoad, 'binary');

    for (const concelho of listConcelhoArr) {
        console.log("get data from CSU concelho",concelho);
        let xlsToLoad2 = json2xls(objResultFindArr.filter(el=>el.Concelho==concelho));

        fs.writeFileSync(`./porConcelho/Lista_PS_CNCSU_Verificar_${(concelho.split(" ")).join("")}.xlsx`, xlsToLoad2, 'binary');
    }

    console.log("finish create excel")
}

loadListaParaVerificar();

//console.log(nimString);