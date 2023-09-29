import { blankPostgresSetting } from "./pg";

export const getBlankSetting = (dbType: string) => {
    switch (dbType) {
      case 'POSTGRES':
        return blankPostgresSetting;
      default:
        return blankPostgresSetting;
    }
  };
  