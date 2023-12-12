# Method to call DataBridges API
import backoff
import httpx
import datetime
from dateutil.relativedelta import relativedelta


class HTTPError(Exception):
    pass


class ApiServerError(Exception):
    pass


class TokenScopeError(Exception):
    pass


class ApiNotAuthorizedError(Exception):
    pass


class NotFoundError(Exception):
    pass


API_ENDPOINTS = {
    'alps': {
        'url': 'vam-data-bridges/1.2.0/MarketPrices/Alps',
        'method': 'GET'
    },
    'markets_list': {
        'url': 'vam-data-bridges/1.2.0/Markets/List',
        'method': 'GET'
    },
    'commodities_list': {
        'url': 'vam-data-bridges/1.2.0/Commodities/List',
        'method': 'GET'
    },
    'commodities_categories_list': {
        'url': 'vam-data-bridges/1.2.0/Commodities/Categories/List',
        'method': 'GET'
    }
}


class WfpApi:
    BASE_URL = 'https://api.wfp.org'

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.tokens_by_scopes = {}

    def _refresh_token(self, scopes):
        resp = httpx.post(f'{self.BASE_URL}/token',
                          data={'grant_type': 'client_credentials', 'scope': ' '.join(scopes)},
                          auth=(self.api_key, self.api_secret))
        resp.raise_for_status()
        resp_data = resp.json()
        received_scopes = set(resp_data['scope'].split(' '))
        if not set(scopes).issubset(received_scopes):
            raise TokenScopeError(f'Could not acquire requested scopes: {scopes}')
        self.tokens_by_scopes[scopes] = resp_data['access_token']

    @backoff.on_exception(backoff.expo, (ApiServerError, ApiNotAuthorizedError), max_tries=8, base=10)
    def _invoke(self, endpoint_name, params=None, body=None):
        if endpoint_name not in API_ENDPOINTS:
            raise ValueError('Invalid endpoint invoked. Check the system configuration')

        endpoint = API_ENDPOINTS.get(endpoint_name)
        if params is None:
            params = {}
        scopes = endpoint.get('scopes', tuple())
        token = self.tokens_by_scopes.get(scopes, '')
        if token == '':
            self._refresh_token(scopes)
            token = self.tokens_by_scopes.get(scopes, '')

        with httpx.Client(base_url=self.BASE_URL) as client:
            headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
            resp = client.request(endpoint['method'], endpoint['url'], params=params, json=body, timeout=None,
                                  headers=headers)

            if resp.status_code == httpx.codes.UNAUTHORIZED:
                self._refresh_token(scopes)
                print('unauthorized. Retrying...')
                raise ApiNotAuthorizedError()
            if resp.status_code >= httpx.codes.INTERNAL_SERVER_ERROR:
                print('Internal server issue. Retrying...')
                raise ApiServerError()
            if resp.status_code == httpx.codes.NOT_FOUND:
                raise NotFoundError()
            if httpx.codes.BAD_REQUEST <= resp.status_code < httpx.codes.INTERNAL_SERVER_ERROR:
                print('Http client issue! Not retrying (as it would be useless)')
                raise HTTPError(f'HTTP Client issue ({resp.status_code})')

        return resp.json()

    def get_market_list(self, iso3):
        page = 1
        all_data = []
        data_cl = None
        while data_cl is None or len(data_cl) > 0:
            # print(f'fetching market page {page}')
            data_cl = self._invoke('markets_list', {'CountryCode': iso3, 'page': page})['items']
            all_data.extend(data_cl)
            page = page + 1
        return all_data

    def get_alps(self, iso3):
        page = 1
        all_data = []
        data_mp = None
        while data_mp is None or len(data_mp) > 0:
            # print(f'fetching alps page {page}')
            # fetch data from 3 months ago
            data_mp = self._invoke('alps', {'CountryCode': iso3, 'page': page, 'startDate': (datetime.datetime.today() - relativedelta(months=3)).strftime('%Y/%m/%d')})['items']
            all_data.extend(data_mp)
            page = page + 1
        return all_data

    def get_commodity_list(self):
        page = 1
        all_data = []
        data_mp = None
        while data_mp is None or len(data_mp) > 0:
            print(f'fetching commodity page {page}')
            data_mp = self._invoke('commodities_list', {'page': page})['items']
            all_data.extend(data_mp)
            page = page + 1
        return all_data

    def get_commodity_category_list(self):
        page = 1
        all_data = []
        data_mp = None
        while data_mp is None or len(data_mp) > 0:
            print(f'fetching commodity category page {page}')
            data_mp = self._invoke('commodities_categories_list', {'page': page})['items']
            all_data.extend(data_mp)
            page = page + 1
        return all_data
pass
