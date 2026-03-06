import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'DataFusion Sharp',
  tagline: '.NET bindings for Apache DataFusion',
  favicon: 'img/favicon.ico',

  url: 'https://nazarii-piontko.github.io',
  baseUrl: '/datafusion-sharp/',

  organizationName: 'nazarii-piontko',
  projectName: 'datafusion-sharp',

  onBrokenLinks: 'throw',

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          path: 'content',
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    navbar: {
      title: 'DataFusion Sharp',
      logo: {
        alt: 'DataFusion Sharp',
        src: 'img/logo-wide.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/nazarii-piontko/datafusion-sharp',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Getting Started', to: '/getting-started/installation'},
            {label: 'Guides', to: '/guides/core-concepts'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'GitHub', href: 'https://github.com/nazarii-piontko/datafusion-sharp'},
            {label: 'NuGet', href: 'https://www.nuget.org/packages/DataFusionSharp'},
          ],
        },
      ],
      copyright: `Copyright ${new Date().getFullYear()} DataFusion Sharp Contributors. Licensed under Apache 2.0.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['csharp', 'bash', 'json'],
    },
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
