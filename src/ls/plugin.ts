import { ILanguageServerPlugin } from '@sqltools/types';
import CloudSpannerDriver from './driver';
import { DRIVER_ALIASES } from './../constants';

const CloudSpannerDriverPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server.getContext().drivers.set(value, CloudSpannerDriver as any);
    });
  }
}

export default CloudSpannerDriverPlugin;
