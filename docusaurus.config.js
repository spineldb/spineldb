// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const {themes} = require('prism-react-renderer');
const lightCodeTheme = themes.github;
const darkCodeTheme = themes.dracula;

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'SpinelDB Documentation',
  tagline: 'A Modern, Redis-Compatible In-Memory Database',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://spineldb.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployments:
  // - If repo is 'spineldb/spineldb.github.io', use baseUrl: '/'
  // - If repo is 'spineldb/spineldb', use baseUrl: '/spineldb/'
  baseUrl: '/',

  // GitHub pages deployment config.
  organizationName: 'spineldb',
  projectName: 'spineldb',

  onBrokenLinks: 'warn',
  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl: 'https://github.com/spineldb/spineldb/tree/main/',
          routeBasePath: '/',
        },
        blog: false,
        theme: {
          customCss: require.resolve('./styles/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/spineldb-social-card.jpg',
      navbar: {
        title: 'SpinelDB',
        logo: {
          alt: 'SpinelDB Logo',
          src: 'img/spineldb-logo.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Documentation',
          },
          {
            href: 'https://github.com/spineldb/spineldb',
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
              {
                label: 'Getting Started',
                to: '/01-installation-and-setup',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/spineldb/spineldb',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} SpinelDB. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: ['rust', 'bash', 'toml'],
      },
    }),
};

module.exports = config;

