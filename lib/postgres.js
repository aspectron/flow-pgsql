const os = require('os');
const fs = require('fs-extra');
const path = require('path');
const mkdirp = require('mkdirp');
const { execFile, execFileSync } = require('child_process');
const { FlowProcess } = require("@aspectron/flow-process");
const utils = require('@aspectron/flow-utils');
const {dpc} = require('@aspectron/flow-async');
// const { Pool, Client } = require('pg');
const isDocker = require('is-docker');
const isDocker_ = isDocker();

const PGSQL_STARTUP_DELAY = 4250; // msec
const PGSQL_TEST_DELAY = 500; // msec

module.exports = class FlowPostgres {

	constructor(options) {

		let missing = [];
		!options.appFolder && missing.push('appFolder');
		!options.dataFolder && missing.push('dataFolder');
		!options.database && missing.push('database');
		!options.user && missing.push('user');
		!options.password && missing.push('password');
		if(missing.length)
			throw new Error(`FlowPostgres::constructor() - options is missing following properties: ${missing.join(' ')}`);

		this.options = options;
		this.flags = utils.args();
		this.appFolder = this.options.appFolder;
		this.dataFolder = this.options.dataFolder;


		const name = this.constructor.name;
		this.log = Function.prototype.bind.call(
			console.log,
			console,
			`%c[${name}]`,
			`font-weight:bold;`
		);

		process.on("SIGINT", async ()=>{
			console.log('SIGINT...')
			if(this.stop) {
				this.stop().then(() => {
					console.log("pgSQL exit ok");
					process.exit();
				}, (err) => {
					console.log("pgSQL exit fail", err);
					process.exit();
				});
			}
			else
				process.exit();
		})		

		process.on("SIGTERM", async ()=>{
			console.log('SIGTERM...')
			if(this.stop) {
				this.stop().then(() => {
					console.log("pgSQL exit ok");
					process.exit();
				}, () => {
					console.log("pgSQL exit fail");
					process.exit();
				});
			}
			else
				process.exit();
		})		

	}

	start() {
		this.log("pgSQL::start()");

		if(os.platform() == 'linux') {
			console.log("pgSQL - skipping bootstrap sequence on linux");
			return Promise.resolve();
		}

		return new Promise(async (resolve,reject) => {

			this.BIN = path.join(this.appFolder,'bin',utils.platform);
			if(!fs.existsSync(this.BIN)) {
				console.log(`FlowPgSQL::postgres::start() - missing folder: ${this.BIN}`);
				return reject(`FlowPgSQL errorr: missing ${this.BIN}`);
			}

			this.PLATFORM_BINARY_EXTENSION = process.platform == 'win32' ? '.exe' : '';

			// this.dataFolder = path.join(os.homedir(),'.project');

			let defaults = {
				datadir : this.dataFolder
			}

			// let args = Object.assign({}, this.task.args || {});

			// args = Object.entries(Object.assign(defaults, args)).map((o) => {
			// 	const [k,v] = o;
			// 	return {k,v};
			// });

			if(isDocker_ || os.platform() == 'linux') {
				this.pgsqlBinFolder = '/usr/bin';
			} else {
				const pgsqlFolder = fs.readdirSync(this.BIN).filter(f => f.match(/^postgresql/i)).shift();
				if(!pgsqlFolder) {
					this.log(`pgSQL - Unable to find 'pgsql' folder in 'bin'`);
					return;
				}
				this.pgsqlBinFolder = path.join(this.BIN, pgsqlFolder, 'bin');
			}
			
			this.binary = { };
			this.binary.postgres = path.join(this.pgsqlBinFolder,'postgres')+this.PLATFORM_BINARY_EXTENSION;
			this.binary.pg_ctl = path.join(this.pgsqlBinFolder,'pg_ctl')+this.PLATFORM_BINARY_EXTENSION;
			this.binary.psql = path.join(this.pgsqlBinFolder,'psql')+this.PLATFORM_BINARY_EXTENSION;
			this.binary.initdb = path.join(this.pgsqlBinFolder,'initdb')+this.PLATFORM_BINARY_EXTENSION;
			this.binary.createdb = path.join(this.pgsqlBinFolder,'createdb')+this.PLATFORM_BINARY_EXTENSION;

			this.dataFolder = path.join(this.dataFolder, 'pgsql',this.options.database);
			this.logsFolder = path.join(this.dataFolder, 'logs');
			[this.dataFolder, this.logsFolder].forEach(f => mkdirp.sync(f));
			//mkdirp.sync(path.join(this.dataFolder, 'logs'));

			//this.logFile = path.join(this.dataFolder, 'logs',`${this.task.key}.log`);
			this.logFile = path.join(this.logsFolder,`pgsql.log`);

			//this.log("CONFIG:".brightRed, this.task.conf);
			const port = this.options.port;

			const args = [
				`-D`,
				this.dataFolder,
				`-p`,
				port,
				// `--port=${port}`,
				// `--log-error=${this.logFile}`,
				// `--user=root`,
				'--timezone=UTC'
				//`--console`
			];

			const run = (...custom_args) => {
				return new Promise((resolve,reject) => {
					dpc(async ()=>{

						this.proc = new FlowProcess({
							verbose : true,
							cwd : os.platform() == 'win32' ? this.dataFolder : '/usr',
							// detached : true,
							args : () => {
								return [
									this.binary.postgres,
									...custom_args,
									...args
								];
							}
						});
						//resolve();
						this.proc.run().then(resolve,reject);
					})
				})
			}

			if(this.flags['reset-pgsql']) {
				this.log("+-","Emptying pgSQL data folder".brightBlue);
				this.log("+-->", this.dataFolder.brightWhite);
				fs.emptyDirSync(this.dataFolder);
			}

			if(!fs.existsSync(path.join(this.dataFolder,'pg_version'))) {
				dpc(async ()=>{
					this.log("+-","pgSQL: initializing data folder".brightYellow,"\n+-->",this.dataFolder.brightWhite);

					const init = new Promise(async (resolve,reject) => {
						try {

							await utils.spawn(this.binary.initdb,[
								`-D`,
								this.dataFolder,
								`-U`,
								`postgres`
							], { 
								cwd : this.dataFolder,
								// stdout : (data) => process.stdout.write(data)
							});

							resolve();
							
						} catch(ex) {
							console.log(ex);
							this.log('FATAL - ABORTING pgSQL STARTUP SEQUENCE! [3]'.brightRed);
							reject(ex);
							return;
						}
							
					});
					
					try {
						await init;
					} catch(ex) {
						console.log(ex);
						this.log('FATAL - ABORTING pgSQL STARTUP SEQUENCE! [4]'.brightRed);
						return;
					}

					await run();
					//this.log("pgSQL PID:", this.proc.process.pid);

//					let dbname = this.options.database;
					const initFile = path.join(this.dataFolder,'init.sql');
					const { user, password, database } = this.options;
					fs.writeFileSync(initFile, `CREATE USER ${user} WITH PASSWORD '${password}';\nCREATE DATABASE ${database};\nGRANT ALL PRIVILEGES ON DATABASE ${database} TO ${user};`);

					//console.log("waiting for 2 seconds")
					dpc(PGSQL_STARTUP_DELAY, async () => {
						this.log("init pgSQL Db".brightYellow);

						const psqlInitArgs = [
							`-v`,
							`ON_ERROR_STOP=1`,
							`--username=postgres`,
							`-p`,
							port,
							`-f`,
							initFile
						];
						try {
							await utils.spawn(this.binary.psql,psqlInitArgs,{
								cwd : this.dataFolder,
								// stdout : (data) => this.writeToSink(data)
							}); 
							this.log("...pgSQL configuration successful!".brightYellow);

							resolve();

							// const createdbArgs = [
							// 	`--username=postgres`,
							// 	'-p', port, '--no-password'
							// ]
							// await utils.spawn(this.binary.psql,psqlInitArgs,{
							// 	cwd : this.dataFolder,
							// 	stdout : (data) => this.writeToSink(data)
							// }); 

							// let client = new Client({
							// 	host : 'localhost', port,
							// 	user : 'flow',
							// 	password: 'flow',
							// });

							// await client.connect();
							
							// await client.query(`CREATE DATABASE IF NOT EXISTS ${this.options.database} DEFAULT CHARACTER SET utf8;`);

							// await client.end();

/*
							const db = mysql.createConnection({
								host : 'localhost', port,
								user : 'flow',
								password: 'flow',
								// database: 'mysql',
								// insecureAuth : true
							});
							
							db.connect(async (err) => {

								if(err) {
									this.log(err);
									this.log("FATAL - MYSQL STARTUP SEQUENCE! [2]".brightRed);
									return reject(err);
								}

								this.log("MySQL connection SUCCESSFUL!".brightGreen);

								if(this.options.database) {
									console.log(`Creating database ${this.options.database}...`)
									db.query(`CREATE DATABASE IF NOT EXISTS ${this.options.database} DEFAULT CHARACTER SET utf8;`, (err) => {
										if(err)
											return reject(err);
										console.log('db creation ok...');
										resolve();
									});        

								}
								else {
									resolve();
								}



								db.end(()=>{
									this.log("MySQL client disconnecting.".brightGreen);
								});
							});
*/
							
							resolve();




						} catch(ex) {
							reject(ex);
						}

					});
				})
			}
			else {
				dpc(async ()=>{
					await run();

					dpc(PGSQL_STARTUP_DELAY, async () => {

						const createdbArgs = [
							`--username=postgres`,'--host=localhost',
							'-p', port, '--no-password', //'--encoding=UTF8',
							//'--echo', 
//							`--owner=aspect`, 
						];

						//if(os.platform() !== 'win32')
						//	createdbArgs.push(`--owner=${os.userInfo().username}`);
							//createdbArgs.push(`--owner=${this.options.user}`);

						createdbArgs.push(this.options.database);

						await utils.spawn(this.binary.createdb,createdbArgs,{
							cwd : this.dataFolder,
							// stdout : (data) => this.writeToSink(data)
						}); 

						resolve();

					});
				})
			}
		})
	}

	stop() {
		this.relaunch = false;
		if(!this.proc) {
			console.log("pgSQL no proc!".brightMagenta);
			return Promise.resolve();
		}

		return new Promise((resolve,reject)=>{

			this.stopped = true;

			// const user = 'flow';
			// const pass = 'flow';
			// const host = 'localhost';
			// const port = this.options.port;

			this.proc.relaunch = false;
			this.proc.no_warnings = true;
			let fail = false;
			const timeout = setTimeout(()=>{
				console.log('pgSQL stop()... timeout');
				if(!fail) {
					fail = true;
					this.log("pgSQL daemon shutdown has timed-out".brightYellow);
					this.proc.terminate().then(resolve, reject);
				}
			}, 15_000);
			
			this.proc.once('halt', () => {
				// console.log(`pgSQL stop ${fail?'fail':'ok'}`);
				if(!fail) {
					this.log("pgSQL daemon has gracefully halted".brightYellow);
					clearTimeout(timeout);
					resolve();
				}
			});


			this.log('pgSQL is being shutdown');
			
			const args = [`stop`,'-D',this.dataFolder];

			// this.log(this.binary.pg_ctl, args);

			try {
				execFile(this.binary.pg_ctl, args, {
					cwd : this.dataFolder
				}, (error,stdout,stderr) => {
					if(error) {
						this.log(`Error stopping pgSQL`.brightRed,`("pg_ctl stop -D ${this.dataFolder}")`.brightWhite);
						this.log(error);
					}			
					
				})
			} catch(ex) {
				console.log(ex,ex.stack);
				fail = true;
				this.log("pgSQL daemon shutdown has timed-out".brightYellow);
				this.proc.terminate().then(resolve, reject);
			}
		})
	}

	log(...args) {
		console.log(...args);
	}
}

