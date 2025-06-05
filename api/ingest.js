import {v4 as uuidv4} from 'uuid';

function generateIngestionId(){
    return Math.random().toString(36).slice(2, 11);
}
const ingestionStore = {};
let batchQueue = [];
function fetchDataForId(id){
    return new Promise((resolve)=>{
        setTimeout(()=>{
            resolve({id, data: "Processed"})
        }, 100)
    })
}
setInterval(async ()=>{
    if(batchQueue.length===0) return;
    const next = batchQueue.shift();
    const {ingestion_id, batch} = next;
    batch.status = "triggered";

    const promises = batch.ids.map(id => fetchDataForId(id));
    await Promise.all(promises)

    batch.status = "completed";

    const allBatches = ingestionStore[ingestion_id].batches;
    const statuses = allBatches.map(b => b.status);
    if (statuses.every(s => s === "completed")) {
        ingestionStore[ingestion_id].status = "completed";
    } else if (statuses.some(s => s === "triggered" || s === "completed")) {
        ingestionStore[ingestion_id].status = "triggered";
    }
}, 5000);

function handler(req, res){
    if(req.method === "POST"){
        const {ids , priority} = req.body;
        if(!ids || !priority){
            return res.status(400).json({message: "All fields are required"});
        }
        if(ids.length===0){
            return res.status(400).json({message: "Invalid ids"});
        }
        const batches = [];
        const ingestion_id = generateIngestionId();
        for (let i=0;i<ids.length;i+=3){
            const batch = {
                batch_id: uuidv4(),
                ids: ids.slice(i, i + 3),
                status: "yet_to_start"
            };
            batches.push(batch);

            batchQueue.push({ ingestion_id, batch });
        }
        ingestionStore[ingestion_id] = {
            ingestion_id,
            priority,
            status: "yet_to_start",
            batches
        };

        return res.status(200).json({ingestion_id});
    } 
    else if(req.method === "GET"){
        const ingestion_id = req.query.ingestion_id;
        const ingestion = ingestionStore[ingestion_id]
        if(!ingestion){
            return res.status(400).json({message: "Missing ingestio id"});
        }
        return res.status(200).json(ingestion);

    } else{
        res.status(405).json({message: `Unable to receive ${req.method} request`});
    }
}

export default handler;