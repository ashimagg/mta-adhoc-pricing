const mongoose = require('mongoose');
const Schema = mongoose.Schema;


const StationSchema = new Schema({
    Station_name: { type: String, default: '' },
    price1: { type: Number, default: '' },
    price2: { type: Number, default: '' },
    entries:  {type:Number, defailt:''},
    entries:  {type:Number, defailt:''},
    exits:  {type:Number, defailt:''},

}, { collection: 'stations' });


module.exports = mongoose.model('Station', StationSchema);