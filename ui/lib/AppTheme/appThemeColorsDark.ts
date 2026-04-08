export const appThemeColorsDark = {
  base: {
    background: {
      normal: '#0A0A0A',
      subtle: '#141414',
    },
    surface: {
      normal: '#1A1A1A',
      hovered: '#2A2A2A',
      selected: '#353535',
    },
    border: {
      subtle: '#333333',
      normal: '#4A4A4A',
      hovered: '#606060',
    },
    fill: {
      normal: '#909090',
      hovered: '#A0A0A0',
    },
    text: {
      lowContrast: '#B8B8B8',
      highContrast: '#FFFFFF',
    },
  },
  accent: {
    background: {
      normal: '#0A1F12',
      subtle: '#0D2515',
    },
    surface: {
      normal: '#113B23',
      hovered: '#164430',
      selected: '#1B5538',
    },
    border: {
      subtle: '#1F6F42',
      normal: '#2A8A5A',
      hovered: '#35A770',
    },
    fill: {
      normal: '#30A46C',
      hovered: '#3CB179',
    },
    text: {
      lowContrast: '#5BC88A',
      highContrast: '#A8E5C2',
    },
  },
  destructive: {
    background: {
      normal: '#1F0A0A',
      subtle: '#2A0F0F',
    },
    surface: {
      normal: '#3F1515',
      hovered: '#4A1A1A',
      selected: '#5A2020',
    },
    border: {
      subtle: '#7A2828',
      normal: '#9A3030',
      hovered: '#BA3838',
    },
    fill: {
      normal: '#E54D2E',
      hovered: '#F05A3C',
    },
    text: {
      lowContrast: '#FF7A5C',
      highContrast: '#FFB5A3',
    },
  },
  warning: {
    background: {
      normal: '#1F1A0A',
      subtle: '#2A2210',
    },
    surface: {
      normal: '#3F3318',
      hovered: '#4A3D1F',
      selected: '#5A4A26',
    },
    border: {
      subtle: '#7A6330',
      normal: '#9A7C3A',
      hovered: '#BA9544',
    },
    fill: {
      normal: '#FFB224',
      hovered: '#FFBE40',
    },
    text: {
      lowContrast: '#FFC85A',
      highContrast: '#FFE0A0',
    },
  },
  positive: {
    background: {
      normal: '#0A1F0A',
      subtle: '#0F2A0F',
    },
    surface: {
      normal: '#153F15',
      hovered: '#1A4A1A',
      selected: '#205A20',
    },
    border: {
      subtle: '#287A28',
      normal: '#309A30',
      hovered: '#38BA38',
    },
    fill: {
      normal: '#46A758',
      hovered: '#52B464',
    },
    text: {
      lowContrast: '#6AC77C',
      highContrast: '#A5E5B0',
    },
  },
  blue: {
    background: {
      normal: '#0A1A2E',
      subtle: '#0F2240',
    },
    surface: {
      normal: '#1A3A5C',
      hovered: '#2A4A6C',
      selected: '#3A5A7C',
    },
    border: {
      subtle: '#4A6A8C',
      normal: '#5A7A9C',
      hovered: '#6A8AAC',
    },
    fill: {
      normal: '#4AB0F0',
      hovered: '#5AC0FF',
    },
    text: {
      lowContrast: '#80C6FF',
      highContrast: '#CCE7FF',
    },
  },
  special: {
    inverted: {
      white: '#000000',
      black: '#FFFFFF',
    },
    fixed: {
      white: '#FFFFFF',
      black: '#000000',
    },
  },
  select: {
    primary: '#30A46C',
    primary25: '#113B23',
    neutral0: '#1A1A1A', // menu background
    neutral80: '#FFFFFF', // text color
  },
} as const;
