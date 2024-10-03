/**
 * SSWW N/query Mass Delete
 * ----------------
 * Accepts a query with two fields, an ID and a type field.
 * Datatype determines which is which.
 * Re-queues until nothing successfully processed.
 * ----------------
 * e.g. SELECT id, 'customrecord_xyz' AS type FROM customrecord_xyz
 *
 * @NScriptType MapReduceScript
 * @NApiVersion 2.1
 * @author Steve Rolfechild
 */
 define(['N/record', 'N/runtime', 'N/file', 'N/task'], function(record, runtime, file, task) {
	 
	 /** !--IMPORTANT--! Set output folder ID for response CSVs !--IMPORTANT--! */
	 const OUTPUT_FOLDER = 123;
	 
	 /** Determines if a value should be considered "empty" or containing no value */
	 function isEmpty(value) { return value == undefined || value == null || (typeof value == 'string' && (value.length == 0 || value == "")); }
	 
	 /**
	  * Pulls headers from cache
	  *
	  * @return string The header line from the CSV file
	  */
	 function outputFilename() { return 'mass_delete_' + Math.floor(new Date().getTime() / 1000) + '.csv'; }
	 
	 function requeue() {
		 let script = runtime.getCurrentScript();
		 let requeueTask = task.create({
			 taskType: task.TaskType.MAP_REDUCE,
			 scriptId: script.id,
			 params: {
				 custscript_sql_query: script.getParameter({ name: 'custscript_sql_query' })
			 }
			 /* deploymentId: 'customdeploy_test_mapreduce_script' */
		 });
		 return requeueTask.submit();
	 }
	 
	 return {
		 
		 /**
		  * Store headers in cache, send CSV file into M/R process
		  */
		 getInputData: function() {
			 let query = runtime.getCurrentScript().getParameter({ name: 'custscript_sql_query' });
			 if (isEmpty(query)) throw 'Missing query';
			 log.debug('Begin mass delete', query);
			 
			 return {
				 type: 'suiteql',
				 query: query
			 }
		 },
		 
		 /**
		  * Parse CSV line, process record update
		  */
		 map: function(context) {
			 let row = { id: undefined, type: undefined, error: undefined };
			 try {
				 let contextValue = JSON.parse(context.value);
				 if (contextValue == undefined || contextValue.values == undefined) throw 'Invalid context :: ' + JSON.stringify(context);
				 if (contextValue.values.length < 2) throw 'Context missing values :: ' + JSON.stringify(contextValue.values);
				 
				 row.id = parseInt(contextValue.values[0]) == contextValue.values[0] ? contextValue.values[0] : contextValue.values[1];
				 row.type = parseInt(contextValue.values[0]) == contextValue.values[0] ? contextValue.values[1] : contextValue.values[0];
				 
				 record.delete({
					 id: row.id,
					 type: row.type
				 });
			 } catch (e) {
				 row.error = e.message;
				 log.error('Unable to delete record', row);
			 }
			 
			 context.write({
				 key: context.key,
				 value: JSON.stringify(row)
			 });
		 },
		 
		 /**
		  * Output CSV result
		  */
		 summarize: function(context) {
			 // Output errors encountered
			 context.mapSummary.errors.iterator().each(function(key, error) {
				 log.error(key, error);
				 return true;
			 });
			 
			 let fname = outputFilename();
			 let csvFile = file.create({
				 name: fname,
				 fileType: file.Type.CSV,
				 folder: OUTPUT_FOLDER
			 });
			 let headers = ['id', 'type', 'error'];
			 csvFile.appendLine({ value: headers.join(', ') });
			 let totalDeleted = 0;
			 context.output.iterator().each(function(key, value) {
				 let v = JSON.parse(value);
				 if (v == undefined || v.id == undefined) log.error('Invalid summary context', value);
				 totalDeleted++;
				 csvFile.appendLine({ value: [row.id, row.type, row.error].join(', ') });
				 return true;
			 });
			 
			 csvFile.save();
			 log.debug('Total Deleted', totalDeleted);
			 log.debug('Output File', fname);
			 
			 // Requeue
			 if (totalDeleted > 0) {
				 log.debug('Requeue', requeue() + ' >> ' + runtime.getCurrentScript().getParameter({ name: 'custscript_sql_query' }));
			 }
		 }
	 }
	 
 });
