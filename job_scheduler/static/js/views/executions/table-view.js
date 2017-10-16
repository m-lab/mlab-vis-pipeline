/**
 * executions-table view.
 *
 * @author wenbin@nextdoor.com
 */

require.config({
  paths: {
    'jquery': 'vendor/jquery',
    'underscore': 'vendor/underscore',
    'backbone': 'vendor/backbone',
    'bootstrap': 'vendor/bootstrap',
    'datatables': 'vendor/jquery.dataTables',

    'utils': 'utils',
    'text': 'vendor/text',
    'execution-result': 'templates/execution-result.html'
  },

  shim: {
    'bootstrap': {
      deps: ['jquery']
    },

    'backbone': {
      deps: ['underscore', 'jquery'],
      exports: 'Backbone'
    },

    'datatables': {
      deps: ['jquery'],
      exports: '$.fn.dataTable'
    }
  }
});

define(['utils',
        'text!execution-result',
        'backbone',
        'bootstrap',
        'datatables'], function(utils, ExecutionResultHtml) {
  'use strict';

  return Backbone.View.extend({
    initialize: function() {
      $('body').append(ExecutionResultHtml);

      this.listenTo(this.collection, 'sync', this.render);
      this.listenTo(this.collection, 'request', this.requestRender);
      this.listenTo(this.collection, 'error', this.requestError);

      this.table = $('#executions-table').dataTable({
        // Sorted by last updated time
        'order': [[3, 'desc']],
        // Disable sorting on result column
        "columnDefs": [
          { "orderable": false, "className": "table-result-column", "targets": 5 }
        ]
      });
    },

    /**
     * Request error handler.
     *
     * @param {object} model
     * @param {object} response
     * @param {object} options
     */
    requestError: function(model, response, options) {
      this.spinner.stop();
      utils.alertError('Request failed: ' + response.responseText);
    },

    /**
     * Event handler for starting to send network request.
     */
    requestRender: function() {
      this.table.fnClearTable();
      this.spinner = utils.startSpinner('executions-spinner');
    },

    /**
     * Render results modal.
     * @param {String} results
     */
    renderResultsModal: function(results) {
      try {

        // if results are in json format, parse them. If they are a stacktrace,
        // this will fail and we'll just put in the content into the error
        // box.
        results = JSON.parse(results);
        results.output = results.output ? utils.replaceNewlines(results.output) : '';
        results.err = results.err ? utils.replaceNewlines(results.err) : '';
        $('#result-box-code').text(results.returncode);
        $('#result-box-output').html(results.output);
        $('#result-box-err').html(results.err);
      } catch (e) {
        results = utils.replaceNewlines(results)
        $('#result-box-err').html(results);
      }

      $('#execution-result-modal').modal();
    },

    /**
     * Event handler for finishing fetching execution data.
     */
    render: function() {
      var executions = this.collection.executions;

      var data = [];
      _.each(executions, function(execution) {
        data.push([
          execution.getNameHTMLString(),
          execution.getStatusHTMLString(),
          execution.getScheduledAtString(),
          execution.getFinishedAtString(),
          execution.getDescription(),
          execution.getResult()
        ]);
      });

      if (data.length) {
        this.table.fnClearTable();
        this.table.fnAddData(data);

        var buttons = $('[data-action=show-result]');
        _.each(buttons, _.bind(function(btn) {
          $(btn).on('click', _.bind(function(e) {
            e.preventDefault();
            var results = decodeURI($(btn).data('content'));
            this.renderResultsModal(results);
          }, this));

          // If there's a query parameter result, we'll display the result.
          if (!_.isUndefined(utils.getParameterByName('result'))) {
            var results = JSON.parse(executions[0].get('result'));
            this.renderResultsModal(results);
          }
        }, this));
      }

      utils.stopSpinner(this.spinner);
    }
  });
});
