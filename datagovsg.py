import httpx
import asyncio
import json
import time

from async_class import AsyncClass

class Async_API_Client(AsyncClass):
    """Class for accessing the data.gov.sg API
    """
    async def __ainit__(self, *args, **kwargs):
        self._url = kwargs.get('url', None)
        self._raw_data = None
        self._data_dict = None
        self._response = None
        self._data_TTL = float(kwargs.get('data_TTL', 0.5)) # Max poll rate
        self._raw_data_timestamp = time.time() - self._data_TTL - 1
        self._status = None

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, x):
        if isinstance(x, str):
            self._url = x
        else:
            self._url = None
    
    @url.deleter
    def url(self):
        self._url = None

    @property
    def status(self):
        return self._status
    
    @property
    def data_TTL(self):
        return self._data_TTL
    
    @data_TTL.setter
    def data_TTL(self, x):
        try:
            self._data_TTL = float(x)
        except Exception as err:
            pass

    async def update_raw_data(self): # Asynchronous request
        """Gets raw API data. 
        Async HTTP request is used to prevent CPU time wastage when waiting for API server response.

        Poll rate limited by setting 

        Args:
            url (string): URL string of the API

        Returns:
            _type_: string containing the raw response of the API
        """
        if self._url is not None :
            if self.raw_data_is_expired():
                async with httpx.AsyncClient() as client:
                    try:
                        self._response = await client.get(self._url)
                        self._raw_data = self._response.text
                        self._data_dict = json.loads(self._raw_data)
                        self._raw_data_timestamp = time.time()
                        self.update_api_status()
                    except httpx.HTTPError as err: #graceful handling of any
                        print("HTTP error, try again later.")
                    except httpx.InvalidURL as err:
                        raise err("URL is not valid")
                    except httpx.StreamError as err:
                        print("Stream error occured. Try again later.")
                    finally:
                        return self._raw_data
            else:
                return self._raw_data
        else:
            pass
            # raise httpx.InvalidURL("URL is None")

    def update_api_status(self):
        """Gets the API status from the raw data

        Args:
            data (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            self._status = self._data_dict.get("api_info").get("status")
        except Exception as err:
            raise err
        finally:
            return self._status

    def raw_data_is_expired(self):
        if time.time() - self._raw_data_timestamp >= self._data_TTL:
            return True
        else:
            return False

class Async_Temperature_API_Client(Async_API_Client):
    
    async def __ainit__(self, *args, **kwargs):
        await super().__ainit__(*args, **kwargs)
        self._location = kwargs.get("location", "Sentosa")

    def get_location_id(self, location, data):
        stations = data.get("metadata").get("stations")
        matches = [station['id'] for station in stations if station["name"] == location]
        
        if len(matches) == 1:
            return matches[0]
        else:
            raise ValueError("Invalid metadata, more than 1 id for same location string.")

    async def get_air_temperature(self):
        await self.update_raw_data()
        if self._status is None: #check data integrity
            raise IOError(f"Malformed response received. Data dump {self._data_dict}")
        if self._status == "healthy":
            # print("API is healthy")
            pass
        else:
            print(f"API is not healthy! URL: {self._url}")

        location_id = self.get_location_id(self._location, self._data_dict)

        matched_readings = [reading for reading in self._data_dict.get("items")[0].get("readings") if reading["station_id"] == location_id]
        if len(matched_readings) == 1:
            temperature = float(matched_readings[0]["value"])
            return temperature
        else:
            raise IOError(f"Malformed response received, response dump:\n {self._raw_data}")

# Main function - do tests here
async def main():
    with open("api_urls/air_temperature.txt") as f:
        url = f.read()
    
    api = await Async_Temperature_API_Client(url=url, data_TTL=1, location="Sentosa")
    for i in range(10000):
        await asyncio.sleep(0.2)
        result = await api.get_air_temperature()
        print(result)
    
if __name__ == "__main__": # Test code here
    asyncio.run(main(), debug=True)