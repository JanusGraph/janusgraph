define(
    [
    ],
    function () {
        // public methods
        return {
            servers : [{
                           serverName : "localhost",
                           uri : BASE_URI + "graphs/"
                       }],
	        getBaseUri : function(ix){
                return this.servers[ix].uri;
            }
        };
    });