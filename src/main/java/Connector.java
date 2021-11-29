import org.json.simple.JSONObject;


public abstract class Connector {
    private JSONObject config;
    protected abstract void parseJSON(JSONObject config);

    public Connector(JSONObject config){
        this.config = config;
        parseJSON(this.config);
    }

    public abstract void execute();
}
