import 'styled-components';
import { AppTheme } from './appTheme';

declare module 'styled-components' {
  export interface DefaultTheme extends AppTheme {}
}
