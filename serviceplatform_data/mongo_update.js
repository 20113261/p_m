db.Task.find({task_name: "detail_hotel_booking_20170928d"}).limit(10000).forEach(
    function (doc) {
        db.Task.update({_id: doc._id}, {$set: {"priority": 10}});
    }
);

db.Task.find({task_name: "detail_hotel_ctrip_20170928d"}).limit(10000).forEach(
    function (doc) {
        db.Task.update({_id: doc._id}, {$set: {"priority": 10}});
    }
);
db.Task.find({task_name: "detail_hotel_hotels_20170928d"}).limit(10000).forEach(
    function (doc) {
        db.Task.update({_id: doc._id}, {$set: {"priority": 10}});
    }
);

db.Task.find({task_name: "detail_hotel_elong_20170928d"}).limit(10000).forEach(
    function (doc) {
        db.Task.update({_id: doc._id}, {$set: {"priority": 10}});
    }
);