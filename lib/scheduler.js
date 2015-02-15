'use strict';

var
  uuid = require('uuid');


var Scheduler = function (store, config) {
  var
    self = this,
    ns = 'https://ns.bergnet.org/boomerang#',
    scheduleIris = {};

  if (!('tasksPerAgent' in config)) {
    config.tasksPerAgent = 5;
  }

  var getDocument = function (iri) {
    var
      documentRegEx = /(#|\/)([^#\/]*)$/,
      iriParts = documentRegEx.exec(iri);

    return iri.substr(0, iri.length - iriParts[0].length);
  };

  /**
   * Returns an array that contains all task IRIs from the task list
   * @param taskListIri
   * @returns {*|Promise}
   */
  this.getTaskIris = function (taskListIri) {
    if (taskListIri == null) {
      taskListIri = config.taskListIri;
    }

    return store.match(getDocument(taskListIri), null, ns + 'hasTask')
      .then(function (graph) {
        var taskIris = [];

        if (graph != null) {
          graph.forEach(function (triple) {
            taskIris.push(triple.object.nominalValue);
          });
        }

        return taskIris;
      });
  };

  /**
   * Returns all tasks in a single Graph object
   * @param taskListIri
   * @returns {*|Promise}
   */
  this.getTasks = function (taskListIri) {
    return self.getTaskIris(taskListIri)
      .then(function (taskIris) {
        return Promise.all(taskIris.map(function (taskIri) {
          return store.graph(getDocument(taskIri));
        }))
      })
      .then(function (taskGraphs) {
        var mergedGraph = rdf.createGraph();

        taskGraphs.forEach(function (taskGraph) {
          mergedGraph.addAll(taskGraph);
        });

        return mergedGraph;
      });
  };

  /**
   * Returns an array of task IRIs with the given status
   * @param status
   * @param taskListIri
   * @returns {*|Promise}
   */
  this.getTaskIrisByStatus = function (status, taskListIri) {
    return self.getTasks(taskListIri)
      .then(function (graph) {
        var taskIris = [];

        graph.match(null, ns + 'status', status).forEach(function (triple) {
          taskIris.push(triple.subject.nominalValue);
        });

        return taskIris;
      });
  };

  /**
   * Sets the status for the given task
   * @param taskIri
   * @param status
   * @returns {*|Promise}
   */
  this.setTaskStatus = function (taskIri, status) {
    return store.graph(getDocument(taskIri))
      .then(function (graph) {
        graph.removeMatches(taskIri, ns + 'status');
        graph.add(rdf.createTriple(
          rdf.createNamedNode(taskIri),
          rdf.createNamedNode(ns + 'status'),
          rdf.createNamedNode(status)));

        return store.add(getDocument(taskIri), graph);
      });
  };

  /**
   * Assigns a task to a task list
   * @param taskIri
   * @param listIri
   * @returns {*|Promise}
   */
  this.addTaskToList = function (taskIri, listIri) {
    return self.setTaskStatus(taskIri, ns + 'assigned')
      .then(function () {
        var graph = rdf.createGraph();

        graph.add(rdf.createTriple(
          rdf.createNamedNode(listIri),
          rdf.createNamedNode(ns + 'hasTask'),
          rdf.createNamedNode(taskIri)));

        return store.merge(getDocument(listIri), graph);
      });
  };

  /**
   * Creates a new task list an deletes existings entries
   * @param taskIris
   * @param listIri
   */
  this.createTaskList = function (taskIris, listIri) {
    store.delete(getDocument(listIri))
      .then(function () {
        return store.add(getDocument(listIri), rdf.createGraph());
      })
      .then(function () {
        return Promise.all(taskIris.map(function (taskIri) {
          return self.addTaskToList(taskIri, listIri);
        }));
      });
  };

  /**
   * Deletes all schedules lists
   * @returns {*}
   */
  this.clean = function () {
    return Promise.all(Object.keys(scheduleIris).map(function (agent) {
      return store.delete(getDocument(scheduleIris[agent]));
    }));
  };

  /**
   * Sets the status of all assigned task back to created
   * @returns {*|Promise}
   */
  this.releaseAssignedTasks = function () {
    return self.getTaskIrisByStatus(ns + 'assigned')
      .then(function (taskIris) {
        return Promise.all(taskIris.map(function (taskIri) {
          return self.setTaskStatus(taskIri, ns + 'created');
        }));
      });
  };

  /**
   * Returns the schedule list IRI for the given agent
   * @param agent
   * @returns {*}
   */
  this.getScheduleIri = function (agent) {
    if (!(agent in scheduleIris)) {
      scheduleIris[agent] = config.scheduleBaseIri + '-' + uuid() + '#list';
    }

    return scheduleIris[agent];
  };

  /**
   * Returns the next n scheduled tasks for the given agent
   * @param agent
   * @param numberOfTasks
   * @returns {*|Promise}
   */
  this.next = function (agent) {
    return Promise.all([
      self.getTaskIrisByStatus(ns + 'assigned', self.getScheduleIri(agent)),
      self.getTaskIrisByStatus(ns + 'created'),
      self.getTaskIris(self.getScheduleIri(agent))
    ])
      .then(function (result) {
        var
          assignedTaskIris = result[0],
          assignableTaskIris = result[1];

        return assignedTaskIris.concat(
          assignableTaskIris.slice(0, Math.max(config.tasksPerAgent-assignedTaskIris.length, 0)));
      });
  };

  /**
   * Schedules the next tasks for the given request options
   * @param reqOptions
   * @returns {*|Promise}
   */
  this.schedule = function (options) {
    return self.next(options.agent)
      .then(function (taskIris) {
        return self.createTaskList(taskIris, self.getScheduleIri(options.agent));
      });
  };
};


module.exports = Scheduler;