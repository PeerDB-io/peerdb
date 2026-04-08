export const appThemeColors = {
  base: {
    background: {
      normal: '#FCFCFC',
      subtle: '#F8F8F8',
    },
    surface: {
      normal: '#F3F3F3',
      hovered: '#EDEDED',
      selected: '#E8E8E8',
    },
    border: {
      subtle: '#E2E2E2',
      normal: '#DBDBDB',
      hovered: '#C7C7C7',
    },
    fill: {
      normal: '#8F8F8F',
      hovered: '#858585',
    },
    text: {
      lowContrast: '#6F6F6F',
      highContrast: '#171717',
    },
  },
  accent: {
    background: {
      normal: '#FBFEFC',
      subtle: '#F2FCF5',
    },
    surface: {
      normal: '#E9F9EE',
      hovered: '#DDF3E4',
      selected: '#CCEBD7',
    },
    border: {
      subtle: '#B4DFC4',
      normal: '#92CEAC',
      hovered: '#5BB98C',
    },
    fill: {
      normal: '#30A46C',
      hovered: '#299764',
    },
    text: {
      lowContrast: '#18794E',
      highContrast: '#153226',
    },
  },
  destructive: {
    background: {
      normal: '#FFFCFC',
      subtle: '#FFF8F7',
    },
    surface: {
      normal: '#FFF0EE',
      hovered: '#f79383',
      selected: '#FDD8D3',
    },
    border: {
      subtle: '#FAC7BE',
      normal: '#F3B0A2',
      hovered: '#EA9280',
    },
    fill: {
      normal: '#E54D2E',
      hovered: '#DB4324',
    },
    text: {
      lowContrast: '#CA3214',
      highContrast: '#341711',
    },
  },
  warning: {
    background: {
      normal: '#FEFDFB',
      subtle: '#FFF9ED',
    },
    surface: {
      normal: '#FFF4D5',
      hovered: '#FFECBC',
      selected: '#FFE3A2',
    },
    border: {
      subtle: '#FFD386',
      normal: '#F3BA63',
      hovered: '#EE9D2B',
    },
    fill: {
      normal: '#FFB224',
      hovered: '#FFA01C',
    },
    text: {
      lowContrast: '#AD5700',
      highContrast: '#4E2009',
    },
  },
  positive: {
    background: {
      normal: '#FBFEFB',
      subtle: '#F3FCF3',
    },
    surface: {
      normal: '#EBF9EB',
      hovered: '#DFF3DF',
      selected: '#CEEBCF',
    },
    border: {
      subtle: '#B7DFBA',
      normal: '#97CF9C',
      hovered: '#B7DFBA',
    },
    fill: {
      normal: '#46A758',
      hovered: '#3D9A50',
    },
    text: {
      lowContrast: '#297C3B',
      highContrast: '#1B311E',
    },
  },
  blue: {
    background: {
      normal: '#F0F8FF',
      subtle: '#E6F3FF',
    },
    surface: {
      normal: '#CCE7FF',
      hovered: '#B3DCFF',
      selected: '#99D1FF',
    },
    border: {
      subtle: '#80C6FF',
      normal: '#4AB0F0',
      hovered: '#2E9AE0',
    },
    fill: {
      normal: '#4AB0F0',
      hovered: '#2E9AE0',
    },
    text: {
      lowContrast: '#1A75C7',
      highContrast: '#0D4A87',
    },
  },
  special: {
    inverted: {
      white: '#FFFFFF',
      black: '#000000',
    },
    fixed: {
      white: '#FFFFFF',
      black: '#000000',
    },
  },
  select: {
    primary: '#30A46C',
    primary25: 'rgba(48, 164, 108, 0.3)',
    neutral0: '#FFFFFF', // menu background
    neutral80: '#171717', // text color
  },
} as const;
