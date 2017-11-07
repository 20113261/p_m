var count = 0;
db.Task.aggregate(
    [
        {
            $match:
                {queue: "file_downloader", finished: 1}
        },
        {
            $group:
                {
                    // _id: {target_url: "$args.target_url", source: "$args.source", source_id: "$args.source_id"}
                    _id: {target_url: "$args.target_url"}
                }
        }
    ]
).map(function (record, index) {
    count++;
});
print(count);