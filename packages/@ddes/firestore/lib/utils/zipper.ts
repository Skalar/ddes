import {promisify} from 'util'
import {gunzip as gunzipCb, gzip as gzipCb} from 'zlib'

export default {
  zip: promisify(gzipCb),
  unzip: promisify(gunzipCb),
}
