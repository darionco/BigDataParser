# BigDataParser
Big data in the browser experiment

### Usage.

- Run `yarn install`
- [Download airport data (Airports2.csv)](https://www.kaggle.com/flashgordon/usa-airport-dataset/version/2)
- Place `Airports2.csv` in `data` folder
- Run `node ./data/extractMetadata.js`
- Run `yarn start`
- In Chrome ([must be chrome](https://caniuse.com/#feat=sharedarraybuffer)) navigate to `localhost:8081`
- Load `data.bin` from the `data` folder
- Apply some filters.
