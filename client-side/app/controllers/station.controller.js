const Station = require('../models/stations.js');

let stationInfo = [];

// Retrieve and return all notes from the database.
exports.findAll = (req, res) => {
 
    console.log("Inside Station!!!")

    Station.find()
        .then(stations => {
            stationInfo = stations;
            res.render('home/index', {
                title: 'MTA Bidding system',
                stations: stations
            });
        }).catch(err => {
            // console.log(err);
            // res.status(304)
            // res.status(500).send({
            //     message: err.message || "Some error occurred while retrieving Stations."
            // });
    });
};
