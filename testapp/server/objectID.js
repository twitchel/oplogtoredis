import { Meteor } from "meteor/meteor";
import objectIDTestCollection from "../imports/api/objectIDTest.js";

import { DDPServer } from "meteor/ddp-server";

// For testing object id behavior
const objectIDHex = "5ae7d0042b2acc1f1796c0b6";
const objectID = new Mongo.Collection.ObjectID(objectIDHex);

Meteor.publish("objectIDTest.pub", function () {
  return objectIDTestCollection.find({
    _id: objectID,
  });
});

Meteor.server.setPublicationStrategy(
  "objectIDTest.pub",
  DDPServer.publicationStrategies.NO_MERGE
);

function initializeFixtures() {
  try {
    objectIDTestCollection.insert({
      _id: objectID,
      value: 0,
    });

    // add a few distractors
    objectIDTestCollection.insert({
      _id: new Mongo.Collection.ObjectID("5ae7d0042b2acc1fdeadbeef"),
      value: 1000,
    });

    objectIDTestCollection.insert({
      _id: "somestring",
      value: 2000,
    });
  } catch (e) {
    if (e.code === 11000) {
      // Ignore -- it was a duplicate key error; some other server just
      // beat us to the insert
      return;
    } else {
      throw e;
    }
  }
}
Meteor.startup(initializeFixtures);

Meteor.methods({
  "objectIDTest.initializeFixtures": initializeFixtures,

  "objectIDTest.increment"() {
    objectIDTestCollection.update(
      {
        _id: objectID,
      },
      {
        $inc: {
          value: 1,
        },
      }
    );
  },
});
