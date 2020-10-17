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
const { Client } = require('pg');

const PGSQL_STARTUP_DELAY = 4250; // msec
const PGSQL_TEST_DELAY = 500; // msec

module.exports = class FlowPostgres {

	constructor(options) {

		let missing = [];
		!options.appFolder && missing.push('appFolder');
		!options.dataFolder && missing.push('dataFolder');
		!options.database && !options.databases && missing.push('database or databases');
		!options.user && !options.users && missing.push('user or users');
		!options.password && !options.users && missing.push('password');
		if(missing.length)
			throw new Error(`FlowPostgres::constructor() - options is missing following properties: ${missing.join(' ')}`);

		this.options = options;
		this.flags = utils.args();
		this.appFolder = this.options.appFolder;
		this.dataFolder = this.options.dataFolder;

        this.log = utils.logger(this.constructor.name);
        //this.log("options", options)
		// const name = this.constructor.name;
		// this.log = Function.prototype.bind.call(
		// 	console.log,
		// 	console,
		// 	`%c[${name}]`,
		// 	`font-weight:bold;`
		// );
		/*
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
		*/
	}

	start() {
		this.log("pgSQL::start()");

		// if(os.platform() == 'linux') {
		// 	console.log("pgSQL - skipping bootstrap sequence on linux");
		// 	return Promise.resolve();
		// }

		return new Promise(async (resolve,reject) => {

			this.PLATFORM_BINARY_EXTENSION = process.platform == 'win32' ? '.exe' : '';

			let defaults = {
				datadir : this.dataFolder
			}

			if(isDocker_ || os.platform() == 'linux') {
				this.pgsqlBinFolder = '/usr/bin';
			} else {

				this.BIN = path.join(this.appFolder,'bin',utils.platform);
				if(!fs.existsSync(this.BIN)) {
					this.log(`::postgres::start() - missing folder: ${this.BIN}`);
					return reject(`FlowPostgres errorr: missing ${this.BIN}`);
				}
	
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
			this.binary.pg_isready = path.join(this.pgsqlBinFolder,'pg_isready')+this.PLATFORM_BINARY_EXTENSION;

			this.dataFolder = path.join(this.dataFolder, 'postgres');// ,this.options.database);
			//this.logsFolder = path.join(this.dataFolder, 'logs');
			//[this.logsFolder].forEach(f => mkdirp.sync(f));
			//mkdirp.sync(path.join(this.dataFolder, 'logs'));

			//this.logFile = path.join(this.dataFolder, 'logs',`${this.task.key}.log`);
			//this.logFile = path.join(this.logsFolder,`pgsql.log`);

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
							//verbose : true,
							//verbose : this.args.verbose,  // true,
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
						this.proc.run().then((pid) => {
							if(this.options.initdb === true)
								return resolve();
							this.waitForConnection()
								.then(resolve,reject);
							//utils.waitForPort(this.options.port, this.options.host).then(resolve,reject);
						},reject);
					})
				})
			}

			if(this.flags['reset-pgsql']) {
				this.log("+-","Emptying pgSQL data folder".brightBlue);
				this.log("+-->", this.dataFolder.brightWhite);
				await fs.remove(this.dataFolder);
			}

			if(!fs.existsSync(path.join(this.dataFolder,'pg_version'))) {
				dpc(async ()=>{
					this.log("+-","pgSQL: initializing data folder".brightYellow,"\n+-->",this.dataFolder.brightWhite);
					const init = new Promise(async (resolve,reject) => {
						if(fs.existsSync(this.dataFolder))
							return reject(`PgSQL needs to initialize it's data folder "${this.dataFolder}", but the folder already exists!  Please make sure the supplied folder doesn't exist.`)

						try {
// console.log('running initdb'.brightGreen);
							await utils.spawn(this.binary.initdb,[
								`-D`,
								this.dataFolder,
								`-U`,
								`postgres`
							], { 
								// cwd : this.dataFolder,
								// stdout : (data) => process.stdout.write(data)
							});
							console.log('initdb done, resolving...'.brightGreen);

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
						this.log(ex);
						this.log('FATAL - ABORTING pgSQL STARTUP SEQUENCE! [4]'.brightRed);
						return;
					}

					await run();
					//this.log("pgSQL PID:", this.proc.process.pid);

					//let dbname = this.options.database;
					const initFile = path.join(this.dataFolder,'init.sql');
					const { user, password, database } = this.options;

					const users = [];
					if(user && password)
						users.push({user,password});
					if(Array.isArray(this.options.users))
						users.push(...this.options.users);

					const user__ = user || users[0];
					const db__ = { };

					const databases = [];
					if(typeof(database) == 'string' && !this.options.databases)
						databases.push({database, user});
					if(Array.isArray(this.options.databases))
						this.options.databases.forEach((db) => {
							if(typeof db == 'string') {
								if(!db__[db]) {
									let [sdb,suser] = db.split(':');
									databases.push({database : db, user : suser || user__});
								}
								db__[db] = true;
							} else if(typeof db == 'object' && db !== null) {
								if(!db__[db.database])
									databases.push(db);
								db__[db.database] = true;
							}
							else {
								console.log(`invalid database option ${JSON.stringify(db)}`);
								throw new Error(`invalid database option ${JSON.stringify(db)}`);
							}
						});

					const u_ = {};
					users.forEach(({user}) => {
						if(u_[user]) {
							console.log(`PgSQL configuration error: duplicate user ${user}`);
							throw new Error(`PgSQL configuration error: duplicate user ${user}`);
						}
						u_[user] = true;
					});

					let initScript = '';
					initScript += users.map(({user,password})=>`CREATE USER ${user} WITH PASSWORD '${password}';`).join('\n')+'\n';
					initScript += databases.map(({database})=>`CREATE DATABASE ${database};`).join('\n')+'\n';
					initScript += databases.map(({database,user})=>!user?``:`GRANT ALL PRIVILEGES ON DATABASE ${database} TO ${user};`).join('\n')+'\n';
					initScript += this.options.sql || '';
// initScript += `CREATE USER ${user} WITH PASSWORD '${password}';
// CREATE DATABASE ${database};
// GRANT ALL PRIVILEGES ON DATABASE ${database} TO ${user};
// `;
					// console.log("-----\ninit:\n",initScript);

					fs.writeFileSync(initFile,initScript);

					//dpc(PGSQL_STARTUP_DELAY, async () => {
					dpc(0, async () => {
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
							console.log('postgres: cleaning up init...'.brightRed);
							await fs.remove(initFile);
							this.log("...pgSQL configuration successful!".brightYellow);


							let pgconf = fs.readFileSync(path.join(this.dataFolder, 'postgresql.conf'), { encoding : 'utf8' });
							console.log(pgconf);
							let pg_settings = {
								unix_socket_directories : '/tmp',
								// unix_socket_group : '',
								// unix_socket_permissions : '0777',
							}
// 							const terms = ['unix_socket_directories' = '/tmp'       # comma-separated list of directories
// 							# (change requires restart)
// #unix_socket_group = ''                 # (change requires restart)
// #unix_socket_permissions = 0777         # begin with 0 to use octal notation
// 			 ']
							const terms = Object.keys(pg_settings);
							let lines = pgcons.split('\n').filter(l=>{
								return !!terms.filter(t=>l.includes(t)).length;
							});
							Object.entries(pg_settings).forEach(([k,v]) => {
								lines.push(`${k} = '${v}'`);
							})
							const text = lines.join('\n');
							fs.writeFileSync(path.join(this.dataFolder, 'postgresql.conf'), text, { encoding : 'utf8' });
							// process.exit(0);
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
							
							//resolve();
						} catch(ex) {
							reject(ex);
						}

					});
				})
			}
			else {
				dpc(async ()=>{
					await run();

					dpc(0, async () => {
						//dpc(PGSQL_STARTUP_DELAY, async () => {

						const createdbArgs = [
							`--username=postgres`,'--host=localhost',
							'-p', port, '--no-password', //'--encoding=UTF8',
							//'--echo', 
							//`--owner=aspect`, 
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
			this.log("pgSQL no proc!".brightMagenta);
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
				this.log('pgSQL stop()... timeout');
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
				this.log(ex,ex.stack);
				fail = true;
				this.log("pgSQL daemon shutdown has timed-out".brightYellow);
				this.proc.terminate().then(resolve, reject);
			}
		})
	}

	// log(...args) {
	// 	console.log(...args);
	// }

	waitForConnection() {
		return new Promise((resolve,reject) => {
			const connect = () => {
				if(1) {
					
					let args = [
						'-h', `${this.options.host}`,
						'-p', `${this.options.port}`,
						'-U', `${this.options.user}`
					]
					utils.spawn(this.binary.pg_isready, args, {
						cwd : this.dataFolder,
						// stdout : (data) => this.writeToSink(data)
					})
					.then(result=>{
						if(result != 0){
							this.log("##waitForConnection##".brightYellow, result)
							return dpc(1000, connect);
						}
						this.log(`connected to ${this.options.host}:${this.options.port}`)
						this.log("##CONNECTION SUCCESS##".brightYellow, result);
						resolve();
					})
					.catch(err=> {
						dpc(1000, connect);
						this.log("##CONNECTION ERROR##".brightYellow, err)
						this.log(err.toString());
					})
				}
				else {
					
					const client = new Client(this.options);
					console.log(`FlowPgSQL connecting to ${this.options.host}:${this.options.port}`)
					client.connect()
					.then(()=>{
						console.log("SELECT 1;".red)
						client.query(`SELECT 1;`)
						.then(()=>{
							this.log(`connected to ${this.options.host}:${this.options.port}`)
							client.end();
							resolve();
						})
						.catch((err) => {
							client.end();
							this.log(err.toString());
							this.log(`connecting to ${this.options.host}:${this.options.port}`);
							this.log(`...waiting for PgSQL service to become available`);
							dpc(1000, connect);
						})
					})
					.catch((err) => {
						client.end();
						dpc(1000, connect);
						console.log("##ERROR##".brightYellow, err)
						this.log(err.toString());
					})
				}
			}

			connect();
		})
	}
}

