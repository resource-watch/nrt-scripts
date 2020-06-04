#!/usr/bin/env python3
if __name__ == '__main__':
    import src
    from src import update_worldbank_data_on_carto
    from src import update_worldbank_layers_on_rw
    update_worldbank_data_on_carto.main()
    update_worldbank_layers_on_rw.main()