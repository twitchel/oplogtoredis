import { Meteor } from "meteor/meteor";
import { Mongo } from "meteor/mongo";
import { check } from "meteor/check";

export const Tasks = new Mongo.Collection("tasks");

if (Meteor.isServer) {
  // This code only runs on the server
  // Only publish tasks that are public or belong to the current user
  import { DDPServer } from "meteor/ddp-server";

  Meteor.publish("tasks", function tasksPublication() {
    return Tasks.find({});
  });

  Meteor.server.setPublicationStrategy(
    "tasks",
    DDPServer.publicationStrategies.NO_MERGE
  );
}

Meteor.methods({
  "tasks.insert"(text) {
    check(text, String);

    Tasks.insert({
      text,
      owner: "testuser",
      username: "Test User",
    });
  },
  "tasks.remove"(taskId) {
    check(taskId, String);
    Tasks.remove(taskId);
  },
  "tasks.setChecked"(taskId, setChecked) {
    check(taskId, String);
    check(setChecked, Boolean);

    Tasks.update(taskId, { $set: { checked: setChecked } });
  },
});
