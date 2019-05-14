const Station = require('../models/stations.js');

// Retrieve and return all notes from the database.
exports.findAll = (req, res) => {

    // req.comment = req.article.comments.find(comment => comment.id === id);

    // if (!req.comment) return next(new Error('Comment not found'));
    console.log("Inside Station!!!", Station)

    Station.find()
        .then(stations => {
            

            res.render('home/index', {
                title: 'MTA Bidding system',
                stations: stations
            });

            // res.send(stations);
        }).catch(err => {
            res.status(500).send({
                message: err.message || "Some error occurred while retrieving Stations."
            });
    });
};
