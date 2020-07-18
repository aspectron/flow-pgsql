const utils  = require('@aspectron/flow-utils');
const {dpc}  = require('@aspectron/flow-async');

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

        this.log = utils.logger(this.constructor.name);
        // const name = this.constructor.name;
        // this.log = Function.prototype.bind.call(
        //     console.log,
        //     console,
        //     `%c[${name}]`,
        //     `font-weight:bold;`
        // );
    }

    async start() {
        this.postgres = new FlowPostgres(this.options);
        await this.postgres.start();
    }

    async stop() {
        return this.postgres?.stop();
    }

    async disconnect() {
        return this.dbPool.end();
    }
// TODO - HANDLE CURSOR PROCESSING
//  https://stackabuse.com/using-postgresql-with-nodejs-and-node-postgres/

    async connect(waitForConnect) {
        let connected_once = false;
        return new Promise((resolve,reject) => {
            this.dbPool = new PgPool(Object.assign({ }, this.options));
            // this.dbPool.on('connect', (client) => {
            //     //  if(!this.postgres.stopped)
            //         //this.log(err);
            //         if(!connected_once && waitForConnect) {
            //             this.log(`FlowPgSQL connected`);
            //             resolve();
            //         }
            //         connected_once = true;
            //         this.log('FlowPgSQL connected');
            // })
            
            this.dbPool.on('error', (err) => {
                //  if(!this.postgres.stopped)
                    this.log(err);
            })

            const connect = () => {
                this.log(`connecting to ${this.options.host}:${this.options.port}`);

                this.dbPool.connect().then((client) => {
                    client.release();
                    this.log(`connected`);
                    resolve();
                }).catch((err) => {
                    this.log(`PgSQL error:`, err.toString());
                    dpc(1000, connect);
                })
            }

            dpc(connect);
            
            this.db = {
                query : async (sql, args) => {
                    if(this.postgres?.stopped)
                        return Promise.reject("pgSQL daemon stopped - the platform is going down!");
                    // this.log("sql:", sql, args)
                    return new Promise((resolve,reject) => {
                        this.dbPool.connect().then((client) => {

                            client.query(sql, args, (err, result) => {
                                client.release();
                                // this.log("FlowPgSQL:",result);

                                if(err) {
                                    this.log("pgSQL Error:".brightRed,err);
                                    return reject(err);
                                }
                                    // this.log("pgSQL GOT ROWS:",rows);
                                resolve(result?.rows);
                            });
                        }, (err) => {
                            this.log(`Error processing pgSQL query:`);
                            this.log(sql);
                            this.log(args);
                            this.log(`SQL Error is: ${err.toString()}`)
                            return reject(err);
                        });
                    });
                }                
            }

            // resolve();
        });
    }

    async sql(...args) {
        if(!this.db)
            return Promise.reject('pgSQL error: ignoring sql query - db interface is not initialized');
        // this.log('SQL:'.brightGreen,args[0]);
        let p = this.db.query(...args);
        p.catch(e=>{
            this.log("sql:exception:", [...args], e);
        })
        return p;
    }

    async index(table, indexes) {
        let idx_list = indexes.slice();
        while(idx_list.length) {
            let [idx, unique] = idx_list.shift().split(':');
            await pg.sql(`CREATE ${unique||''} INDEX IF NOT EXISTS ${table}_idx_${idx} ON ${table} (${idx})`);
        }
        return Promise.resolve();
    }
}

module.exports = FlowPgSQL;

