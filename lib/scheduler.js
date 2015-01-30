'use strict';


var Scheduler = function (store, taskListIri) {
  var
    self = this,
    ns = 'https://ns.bergnet.org/boomerang#';

  var getDocument = function (iri) {
    var
      documentRegEx = /(#|\/)([^#\/]*)$/,
      iriParts = documentRegEx.exec(iri);

    return iri.substr(0, iri.length - iriParts[0].length);
  };

  this.listTaskIris = function () {
    return store.match(getDocument(taskListIri), taskListIri, ns + 'hasTask')
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

  this.listTaskIrisByStatus = function (status) {
    return self.listTasks()
      .then(function (graph) {
        var taskIris = [];

        graph.match(null, ns + 'status', status).forEach(function (triple) {
          taskIris.push(triple.subject.nominalValue);
        });

        return taskIris;
      });
  };

  this.listTasks = function () {
    return self.listTaskIris()
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

  this.next = function (agent, numberOfTasks) {
    if (numberOfTasks == null) {
      numberOfTasks = 1;
    }

    return self.listTaskIrisByStatus(ns + 'created')
      .then(function (taskIris) {
        return taskIris.slice(0, numberOfTasks);
      });
  };

  this.add = function (taskIris, listIri) {
    var graph = rdf.createGraph();

    var createTriple = function (taskIri) {
      graph.add(rdf.createTriple(
        rdf.createNamedNode(listIri),
        rdf.createNamedNode(ns + 'hasTask'),
        rdf.createNamedNode(taskIri)
      ));
    };

    taskIris.forEach(function (taskIri) {
      createTriple(taskIri);
    });

    return graph;
  };

  this.schedule = function (scheduleListIri, options) {
    return self.next(null, 10)
      .then(function (taskIris) {
        return store.add(getDocument(scheduleListIri), self.add(taskIris, scheduleListIri));
      });
  };
};


module.exports = Scheduler;