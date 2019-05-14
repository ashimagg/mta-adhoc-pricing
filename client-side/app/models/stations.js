const mongoose = require('mongoose');
const Schema = mongoose.Schema;


const StationSchema = new Schema({
    Station_name: { type: String, default: '' },
    price1: { type: Number, default: '' },
    price2: { type: Number, default: '' }, 
}, { collection: 'stations' });


module.exports = mongoose.model('Station', StationSchema);