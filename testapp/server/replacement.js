import { Meteor } from "meteor/meteor";
import replacementTestCollection from "../imports/api/replacementTest.js";
import insertIgnoreDupKey from "../imports/api/insertIgnoreDupKey.js";

import { DDPServer } from "meteor/ddp-server";

// For testing replacement modification
Meteor.publish("replacementTest.pub", function () {
  return replacementTestCollection.find(
    {
      a: "a",
    },
    {
      fields: { a: true },
    }
  );
});

Meteor.server.setPublicationStrategy(
  "replacementTest.pub",
  DDPServer.publicationStrategies.NO_MERGE
);

function initializeFixtures() {
  insertIgnoreDupKey(replacementTestCollection, {
    _id: "test",
    a: "a",
  });
}

Meteor.startup(initializeFixtures);

Meteor.methods({
  "replacementTest.initializeFixtures": initializeFixtures,

  "replacementTest.change"(key) {
    replacementTestCollection.update(
      {
        _id: "test",
      },
      {
        [key]: key,
      }
    );
  },
});
