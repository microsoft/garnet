// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Garnet',
  tagline: 'A high-performance cache-store from Microsoft Research',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://microsoft.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/garnet/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'microsoft', // Usually your GitHub org/user name.
  projectName: 'garnet', // Usually your repo name.
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  
  markdown: {
    mermaid: true,
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/microsoft/garnet/tree/main/website/',
        },
        blog: {
          showReadingTime: true,
          editUrl: 'https://github.com/microsoft/garnet/tree/main/website/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],

  plugins: [
    "docusaurus-plugin-clarity",
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      mermaid: {
        theme: {light: 'neutral', dark: 'dark'},
      },
      // Replace with your project's social card
      //image: 'img/docusaurus-social-card.jpg',
      navbar: {
        title: 'Garnet',
        logo: {
          alt: 'Garnet Logo',
          src: 'img/garnet-logo-diamond.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'garnetDocSidebar',
            position: 'left',
            label: 'Docs',
          },
          {to: '/blog', label: 'Blog', position: 'left'},
          {
            href: 'https://github.com/microsoft/garnet',
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
                to: '/docs/getting-started',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Discord',
                href: 'https://aka.ms/garnet-discord',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/msrgarnet',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/microsoft/garnet',
              },
            ],
          },
        ],
          copyright: `<p class="text-center">
          <a href="
          https://go.microsoft.com/fwlink/?LinkId=521839"
          style="color: white;">Privacy &amp; Cookies</a> |
          <a href="
          https://go.microsoft.com/fwlink/?LinkID=2259814"
          style="color: white;">Consumer Health Privacy</a> |
          <a href="
          https://go.microsoft.com/fwlink/?LinkID=206977"
          style="color: white;">Terms of Use</a> |
          <a href="
          https://www.microsoft.com/trademarks"
          style="color: white;">Trademarks</a> | © 2025
          </p>`,
      },
      prism: {
        additionalLanguages: ['csharp'],
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
      colorMode: {
        defaultMode: 'light',
        disableSwitch: false,
        respectPrefersColorScheme: false,
      },
      clarity: {
        ID: "loh6v65ww5",
      },
      // github codeblock theme configuration
        codeblock: {
            showGithubLink: true,
            githubLinkLabel: 'View on GitHub',
            showRunmeLink: false,
            runmeLinkLabel: 'Checkout via Runme'
      },
    }),
  themes: [
    '@docusaurus/theme-mermaid',
    'docusaurus-theme-github-codeblock',
    [
      require.resolve("@easyops-cn/docusaurus-search-local"),
      {
        hashed: true,
        blogDir: "./blog/",
      },
    ],
  ],
};

export default config;
