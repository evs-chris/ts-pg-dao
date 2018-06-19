# ts-pg-dao

Do you find ORMs a bit limiting when it comes to interfacing with PostgreSQL? Would you rather just have a way to marshal data back and forth between the database and your app using whatever wacky queries you want? Do you want the full power of the lovely node-postgres module available to your code with a layer of typing on top? Well this module can help with that.

## Config

Since this is a __code generator__, it takes a config file, written in typescript, and spits out source files based on its contents. This module has two sections, a config-time and a runtime. The runtime is very slight and is used to add a tiny bit of sugar on top of node-postgres. The config-time gives you some tools to make defining your data access objects a little easier.

Your config file should be named `ts-pg-dao.config.ts` by default, though you can specify any name you want using the `--config` option of the CLI.

In your config, you can import `@evs-chris/ts-pg-dao` to access config helpers, probably as `import * as dao from '@evs-chris/ts-pg-dao'`. The config file is expected to export a `Config` object as its default export, and there's a handy wrapper function `dao.config` to help you do so.

The `config` method takes a connection object and a database callback function and lets you build a config. The database callback takes a `Builder` and should return a `Config`. The `Config` interface has and `output` that either specifies a path to write server DAO source or an object that specifies where to dump `server` and `client` DAO sources. It should also contain an array of `models`, and it can specify whether or not an `index` source file should be generated for each set of DAO sources.

The builder has methods to read a `table`, which the `dao.Model.from` method will conveniently turn into a pre-populated `Model` definition. Each model requires a primary key or one or more columns, which can be specified manually if the helper can't detect it for some reason. It can also specify a `concurrent` field, which is an "optimistic lock" field that allows the DAO to know if a record is changed while trying to save it. Fields in the `Model` can also have aliases assigned, and there are a few other discoverable things you can do with a model if you poke around using an editor that has typescript completion enabled.

The `Model` config also has a `query` builder that allows you to specify a method name, some sql, some params, an include map, a result type name, and whether or not the query should return a single result. This will generate a query method and, optionally, a return type that is useful for type safety with the result on both the server and client.

## Runtime

On the runtime side, you get access to `findById`, `findOne`, `findAll`, and `save` methods in addition to any query methods specified in your config for a given model. The `save` method will either update or insert the record depending on the presence and value of its primary key(s) and concurrency field(s). Any fields not present will simply be left out of the query, and any fields that have server-side values (defaults) will be set back on the model from the result of the query, meaning that the model will come back with primary keys (an other fields) intact if it is a new model.

__Note__: you can override the `findById` method by supplying a query with the `name` set to `findById`. This gives you an opportunity to easily pull in associated models with the main record.

The `where` string for the `findOne` and `findAll` methods is simply concatenated to an appropriate select statement, and the parameters passed after the where clause are handed off to node-postgres.

Queries get a bit more treatment, as their parameters are collected into a params object type that is used by the generated method to supply requested params to the query. Pre-processing on the query at config-time lets you know if there are any errors in your sql, as it will try to prepare each query after processing them for and table and field aliases and parameters. Parameters should be referenced by name in the query.

### Connection

The `Client` interface from node-postgres needs a slight enhancement to work with DAO classes, so that it can manage transactions a little more easily. `begin`, `commit`, and `rollback` methods and an `inTransaction` property are added to a `Client` passed to `enhance` from `import { enhance } from '@evs-chris/ts-pg-dao/runtime';`.

## Slightly special query SQL

The sql specified in query config has a few slightly special features to allow the generated code to process additional models included in each result. Model tables should be referenced in a from or join clause with an `@` prepended e.g. `select @books.* from @books ...`. If the model is not the primary table, then its alias will determine its field name in the output type e.g. `select @books.*, @author.* from @books join @authors author on books.author_id = author.id`.

__Note__: `@books.*` is shorthand for all of the fields specified in the model, because the fields are each aliases to avoid ambiguous field references.

Fields in a query sql may also be referenced by their table alias using the prefixed `@`, which allows the query preprocessor to sub in an appropriate alias for the field e.g. `@books.id` becomes `books.id as __books_id` or something along those lines. A field prefixed with `@:` will be turned directly into its alias e.g. `@:books.id` becomes `__books_id`, which is useful for things like common table expressions where you need to reference fields from one of the component queries in other component queries.

Parameters are simply referenced by their name prefixed with a `$` e.g. `select @books.* from @books where id = $id` if the param is created as `dao.param('id')`. The parameters are collected and substituted with appropriate numbered params to be passed on to node-postgres, and a param object mapper is set up in the generated code that passes the appropriate params in the appropriate positions at runtime. For rerefence, if that query where named `findABook`, it would be called as `Books.findABook(connection, { id: 'some id' })`.