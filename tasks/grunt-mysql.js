var fs = require('fs');
var path = require('path');
var _ = require('lodash');
var mysql = require('mysql');

function createTask(grunt) {
  grunt.registerMultiTask('mysql', 'Run MySQL scripts.', 

  function mySqlTask() {
	
    var self = this;
    var options = getOptions();

    validateOptions(options);

    if (self.filesSrc == null || self.filesSrc.length == 0) {
      grunt.fatal('No source files specified but at least one must be. Source files are the sql scripts to run.');
      return;
    }

    var done = self.async();

    var connection = mysql.createConnection(options);

    var sqlObjects = _.map(self.filesSrc, function(fileSrc) {
      return {filePath: fileSrc, sql: fs.readFileSync(fileSrc, 'utf8')};
    });

    connection.connect(function(err) {
      if (err) {
        grunt.fatal(err);
        return;
      }

      var sqlProcessedCount = 0;
      var errorAlready = false;

      sqlObjects.forEach(function (sqlObject) {
        if (errorAlready) {
          return;
        }
        var startTime = new Date();

        if (options.sqlIsTemplate) {
          sqlObject.sql = grunt.template.process(sqlObject.sql);
        }
        connection.query(sqlObject.sql, function(err) {
          if (err) {
            err.message = 'An error occurred running SQL script \'' + sqlObject.filepath + '\': ' + err.message;
            errorAlready = true;
            closeConnection(err);
            return;
          }

          var endTime = new Date();

          grunt.log.write('Executed sql file \'' + sqlObject.filePath + '\' in ' + (endTime.valueOf() - startTime.valueOf()) + 'ms.');

          sqlProcessedCount++;
          if (sqlProcessedCount == sqlObjects.length) {
            closeConnection();
          }
        });
      });
    });

    function closeConnection(queryErr) {
      connection.end(function(endErr) {
        done(queryErr || endErr);
      });
    }  

  function getOptions() {
    var options = self.options({
      configPath: null,
      host: 'localhost',
      database: null,
      user: null,
      password: null,
      port: 3306,
      sqlIsTemplate: false
    });

    if (options.configPath != null && options.configPath.length) {
      var fullConfigPath = path.resolve(options.configPath);
      if (!fs.existsSync(fullConfigPath)) {
        grunt.warn('configPath option specified but was not resolved to a valid file. Configured path is \'' +
          options.configPath +
          '\' which resolved to \'' + fullConfigPath + '\'.');
      } else {
        var configText = fs.readFileSync(fullConfigPath, 'utf8');
        var configObject;

        try {
          configObject = JSON.parse(configText);
        }
        catch(ex) {
          grunt.fail.warn('Unable to parse config file from \'' + fullConfigPath + '\' as JSON: ' + ex.message);
          configObject = null;
        }

        if (configObject != null) {
          options = _.assign(options, configObject);
        }
      }
    }

    return options;
  }

  function validateOptions(options) {

    // not using grunt.config.requires since we provided an option to load config from a separate file

    ['host', 'user', 'password'].forEach(function(option) {
      if (options[option] == null) {
        grunt.fatal(option + ' option is required but none was specified.');
      }
    });
  }
  });
}


module.exports = createTask;