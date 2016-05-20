package org.shisoft.morpheus;

import clojure.lang.Obj;
import net.openhft.koloboke.collect.map.hash.*;

/**
 * Created by shisoft on 24/2/2016.
 */
public class SchemaStore {
    HashIntObjMap schemaIdMap = HashIntObjMaps.newMutableMap();
    HashObjIntMap snameIdMap = HashObjIntMaps.newMutableMap();
    HashIntObjMap nebIdschemaMap = HashIntObjMaps.newMutableMap();

    public HashIntObjMap getSchemaIdMap() {
        return schemaIdMap;
    }

    public int put (int id, Integer nebSchemaId, Object sname, Object schema){
        this.schemaIdMap.put(id, schema);
        this.snameIdMap.put(sname, id);
        if (nebSchemaId != null) {
            this.nebIdschemaMap.put(nebSchemaId.intValue(), schema);
        }
        return id;
    }

    public boolean snameExists (Object sname){
        return this.snameIdMap.containsKey(sname);
    }

    public  void clear(){
        this.snameIdMap.clear();
        this.snameIdMap.clear();
        this.nebIdschemaMap.clear();
    }

    public int sname2Id (Object sname){
        return snameIdMap.getInt(sname);
    }

    public Object nebId2schema (int nebId) {
        return this.nebIdschemaMap.get(nebId);
    }

    public Object getById (int id) {
        return schemaIdMap.get(id);
    }
}
