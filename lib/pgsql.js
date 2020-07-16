const µFabric   = require('micro-fabric');
const utils  = require('@aspectron/flow-utils');

const { Pool : PgPool } = require('pg');
const FlowPostgres = require('./postgres');


class FlowPgSQL {

    constructor(options) {
        this.args = utils.args();
        this.options = Object.assign({
            host : 'localhost',
            port : 5432,
            user : 'postgres',
            password : '',
        }, options || { });;

        if(!this.options.database)
            throw new Error(`FlowPgSQL::constructor() requires 'options.database' property`);
    }

    async start() {
        const postgres = this.postgres = new FlowPostgres(this.options);
        await postgres.start();
    }

// TODO - HANDLE CURSOR PROCESSING
//  https://stackabuse.com/using-postgresql-with-nodejs-and-node-postgres/

    async init() {

        return new Promise((resolve,reject) => {
            this.dbPool = new PgPool(Object.assign({ }, this.options));
            this.dbPool.on('error', (err) => {
                //  if(!this.postgres.stopped)
                    console.log(err);
            })
            
            this.db = {
                query : async (sql, args) => {
                    if(this.postgres?.stopped)
                        return Promise.reject("pgSQL daemon stopped - the platform is going down!");
                    // console.log("sql:", sql, args)
                    return new Promise((resolve,reject) => {
                        this.dbPool.connect().then((client) => {

                            client.query(sql, args, (err, result) => {
                                client.release();
                                // console.log("FlowPgSQL:",result);

                                if(err) {
                                    console.log("pgSQL Error:".brightRed,err);
                                    return reject(err);
                                }
                                    // console.log("pgSQL GOT ROWS:",rows);
                                resolve(result?.rows);
                            });
                        }, (err) => {
                            console.log(`Error processing pgSQL query:`);
                            console.log(sql);
                            console.log(args);
                            console.log(`SQL Error is: ${err.toString()}`)
                            return reject(err);
                        });
                    });
                }                
            }

            resolve();
        });
    }

    async sql(...args) {
        if(!this.db)
            return Promise.reject('pgSQL error: ignoring sql query - db interface is not initialized');
        // console.log('SQL:'.brightGreen,args[0]);
        let p = this.db.query(...args);
        p.catch(e=>{
            console.log("sql:exception:", [...args], e);
        })
        return p;
    }
}

module.exports = FlowPgSQL;

