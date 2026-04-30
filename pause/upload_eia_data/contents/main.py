#!/usr/bin/env python3
if __name__ == '__main__':
    from src import update_eia_data_on_carto
    from src import update_eia_layers_on_rw
    update_eia_data_on_carto.main()
    update_eia_layers_on_rw.main()