def layer_def(ifc_type='CS', most_recent=0):
    layerConfig = {
                      "body": {
                        "layers": [
                          {
                            "options": {
                              "cartocss_version": "2.3.0",
                              "cartocss": "#layer {polygon-opacity:1; line-width:1; line-color:#FFF; line-opacity:1;} [ifc=5]{polygon-fill:#bd0026;} [ifc=4]{polygon-fill:#f03b20;} [ifc=3] {polygon-fill:#fd8d3c;} [ifc=2] {polygon-fill:#fecc5c;} [ifc=1]{polygon-fill:#ffffb2;}",
                              "sql": "SELECT * FROM foo_003_fews_net_food_insecurity where start_date = (SELECT distinct start_date from (SELECT start_date, dense_rank() over (order by start_date desc) as rn from foo_003_fews_net_food_insecurity where ifc_type = {}) t where rn={}) and ifc_type = {}".format(ifc_type, most_recent, ifc_type)
                            },
                            "type": "mapnik"
                          }
                        ],
                        "minzoom": 3,
                        "maxzoom": 18
                      },
                      "account": "rw-nrt"
                    }

    legendConfig =  {
                      "type": "choropleth",
                      "items": [
                        {
                          "name": "None/Minimal",
                          "color": "#ffffb2"
                        },
                        {
                          "name": "Stressed",
                          "color": "#fecc5c"
                        },
                        {
                          "name": "Crisis",
                          "color": "#fd8d3c"
                        },
                        {
                          "name": "Emergency",
                          "color": "#f03b20"
                        },
                        {
                          "name": "Humanitarian Catastrophe/Famine",
                          "color": "#bd0026"
                        }
                      ]
                    }

    return layerConfig, legendConfig
