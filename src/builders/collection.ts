import type { Client } from "../client";

export class CollectionQueryBuilder<T extends Client, Collection, CollectionsTypes> {

    constructor(client: T, { }: { collectionId: string, schema: string }) {

    }

    async find(){
        
    }

}
