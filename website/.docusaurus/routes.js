import React from 'react';
import ComponentCreator from '@docusaurus/ComponentCreator';

export default [
  {
    path: '/garnet/__docusaurus/debug',
    component: ComponentCreator('/garnet/__docusaurus/debug', 'ace'),
    exact: true
  },
  {
    path: '/garnet/__docusaurus/debug/config',
    component: ComponentCreator('/garnet/__docusaurus/debug/config', 'b10'),
    exact: true
  },
  {
    path: '/garnet/__docusaurus/debug/content',
    component: ComponentCreator('/garnet/__docusaurus/debug/content', '839'),
    exact: true
  },
  {
    path: '/garnet/__docusaurus/debug/globalData',
    component: ComponentCreator('/garnet/__docusaurus/debug/globalData', '4ac'),
    exact: true
  },
  {
    path: '/garnet/__docusaurus/debug/metadata',
    component: ComponentCreator('/garnet/__docusaurus/debug/metadata', '8e9'),
    exact: true
  },
  {
    path: '/garnet/__docusaurus/debug/registry',
    component: ComponentCreator('/garnet/__docusaurus/debug/registry', '46c'),
    exact: true
  },
  {
    path: '/garnet/__docusaurus/debug/routes',
    component: ComponentCreator('/garnet/__docusaurus/debug/routes', '54b'),
    exact: true
  },
  {
    path: '/garnet/blog',
    component: ComponentCreator('/garnet/blog', 'dc2'),
    exact: true
  },
  {
    path: '/garnet/blog/archive',
    component: ComponentCreator('/garnet/blog/archive', 'b6d'),
    exact: true
  },
  {
    path: '/garnet/blog/authors',
    component: ComponentCreator('/garnet/blog/authors', 'b4f'),
    exact: true
  },
  {
    path: '/garnet/blog/brief-history',
    component: ComponentCreator('/garnet/blog/brief-history', '362'),
    exact: true
  },
  {
    path: '/garnet/blog/etags-when-and-how',
    component: ComponentCreator('/garnet/blog/etags-when-and-how', 'd1e'),
    exact: true
  },
  {
    path: '/garnet/blog/msr-blog-announcement',
    component: ComponentCreator('/garnet/blog/msr-blog-announcement', 'c52'),
    exact: true
  },
  {
    path: '/garnet/blog/tags',
    component: ComponentCreator('/garnet/blog/tags', '3ac'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/announcement',
    component: ComponentCreator('/garnet/blog/tags/announcement', '933'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/caching',
    component: ComponentCreator('/garnet/blog/tags/caching', 'e07'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/concurrency',
    component: ComponentCreator('/garnet/blog/tags/concurrency', 'f2c'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/etags',
    component: ComponentCreator('/garnet/blog/tags/etags', '806'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/garnet',
    component: ComponentCreator('/garnet/blog/tags/garnet', '102'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/history',
    component: ComponentCreator('/garnet/blog/tags/history', 'b41'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/introduction',
    component: ComponentCreator('/garnet/blog/tags/introduction', '0af'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/lock-free',
    component: ComponentCreator('/garnet/blog/tags/lock-free', 'c23'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/msr',
    component: ComponentCreator('/garnet/blog/tags/msr', 'bb8'),
    exact: true
  },
  {
    path: '/garnet/blog/tags/oss',
    component: ComponentCreator('/garnet/blog/tags/oss', '031'),
    exact: true
  },
  {
    path: '/garnet/markdown-page',
    component: ComponentCreator('/garnet/markdown-page', '442'),
    exact: true
  },
  {
    path: '/garnet/search',
    component: ComponentCreator('/garnet/search', '921'),
    exact: true
  },
  {
    path: '/garnet/docs',
    component: ComponentCreator('/garnet/docs', 'ab9'),
    routes: [
      {
        path: '/garnet/docs',
        component: ComponentCreator('/garnet/docs', 'f09'),
        routes: [
          {
            path: '/garnet/docs',
            component: ComponentCreator('/garnet/docs', '70a'),
            routes: [
              {
                path: '/garnet/docs',
                component: ComponentCreator('/garnet/docs', '5c8'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/azure/api-compatibility',
                component: ComponentCreator('/garnet/docs/azure/api-compatibility', 'd83'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/azure/cluster-configuration',
                component: ComponentCreator('/garnet/docs/azure/cluster-configuration', '2d2'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/azure/faq',
                component: ComponentCreator('/garnet/docs/azure/faq', 'cc7'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/azure/monitoring',
                component: ComponentCreator('/garnet/docs/azure/monitoring', 'b9c'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/azure/overview',
                component: ComponentCreator('/garnet/docs/azure/overview', 'e50'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/azure/quickstart',
                component: ComponentCreator('/garnet/docs/azure/quickstart', 'a38'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/azure/resiliency',
                component: ComponentCreator('/garnet/docs/azure/resiliency', '5a0'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/azure/security',
                component: ComponentCreator('/garnet/docs/azure/security', 'f8a'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/benchmarking/overview',
                component: ComponentCreator('/garnet/docs/benchmarking/overview', '1e0'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/benchmarking/resp-bench',
                component: ComponentCreator('/garnet/docs/benchmarking/resp-bench', '6d2'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/benchmarking/results-resp-bench',
                component: ComponentCreator('/garnet/docs/benchmarking/results-resp-bench', '40b'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/cluster/key-migration',
                component: ComponentCreator('/garnet/docs/cluster/key-migration', '3a2'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/cluster/overview',
                component: ComponentCreator('/garnet/docs/cluster/overview', 'cc0'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/cluster/replication',
                component: ComponentCreator('/garnet/docs/cluster/replication', '765'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/acl',
                component: ComponentCreator('/garnet/docs/commands/acl', '7af'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/analytics',
                component: ComponentCreator('/garnet/docs/commands/analytics', '896'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/api-compatibility',
                component: ComponentCreator('/garnet/docs/commands/api-compatibility', '0db'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/checkpoint',
                component: ComponentCreator('/garnet/docs/commands/checkpoint', '1d2'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/client',
                component: ComponentCreator('/garnet/docs/commands/client', 'e83'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/cluster',
                component: ComponentCreator('/garnet/docs/commands/cluster', 'fe3'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/data-structures',
                component: ComponentCreator('/garnet/docs/commands/data-structures', 'e6a'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/garnet-specific-commands',
                component: ComponentCreator('/garnet/docs/commands/garnet-specific-commands', '091'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/generic',
                component: ComponentCreator('/garnet/docs/commands/generic', 'bb2'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/json',
                component: ComponentCreator('/garnet/docs/commands/json', 'cbf'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/overview',
                component: ComponentCreator('/garnet/docs/commands/overview', '221'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/raw-string',
                component: ComponentCreator('/garnet/docs/commands/raw-string', '4b5'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/scripting',
                component: ComponentCreator('/garnet/docs/commands/scripting', '4fc'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/server',
                component: ComponentCreator('/garnet/docs/commands/server', '77a'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/commands/transactions',
                component: ComponentCreator('/garnet/docs/commands/transactions', '726'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/cluster',
                component: ComponentCreator('/garnet/docs/dev/cluster', '800'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/garnet/docs/dev/cluster/overview',
                component: ComponentCreator('/garnet/docs/dev/cluster/overview', '283'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/cluster/sharding',
                component: ComponentCreator('/garnet/docs/dev/cluster/sharding', 'a39'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/cluster/slot-migration',
                component: ComponentCreator('/garnet/docs/dev/cluster/slot-migration', '025'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/code-structure',
                component: ComponentCreator('/garnet/docs/dev/code-structure', '1d6'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/collection-broker',
                component: ComponentCreator('/garnet/docs/dev/collection-broker', '1c9'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/configuration',
                component: ComponentCreator('/garnet/docs/dev/configuration', 'e80'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/contributing',
                component: ComponentCreator('/garnet/docs/dev/contributing', '599'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/custom-commands',
                component: ComponentCreator('/garnet/docs/dev/custom-commands', '41b'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/garnet-api',
                component: ComponentCreator('/garnet/docs/dev/garnet-api', '1b5'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/multi-db',
                component: ComponentCreator('/garnet/docs/dev/multi-db', 'd52'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/network',
                component: ComponentCreator('/garnet/docs/dev/network', '2f8'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/onboarding',
                component: ComponentCreator('/garnet/docs/dev/onboarding', 'c3a'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/processing',
                component: ComponentCreator('/garnet/docs/dev/processing', '27c'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/transactions',
                component: ComponentCreator('/garnet/docs/dev/transactions', 'b62'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/tsavorite/epoch',
                component: ComponentCreator('/garnet/docs/dev/tsavorite/epoch', '066'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/tsavorite/intro',
                component: ComponentCreator('/garnet/docs/dev/tsavorite/intro', 'd12'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/tsavorite/locking',
                component: ComponentCreator('/garnet/docs/dev/tsavorite/locking', '9fb'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/tsavorite/reviv',
                component: ComponentCreator('/garnet/docs/dev/tsavorite/reviv', '4ec'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/dev/tsavorite/storefunctions',
                component: ComponentCreator('/garnet/docs/dev/tsavorite/storefunctions', 'a2e'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/extensions/module',
                component: ComponentCreator('/garnet/docs/extensions/module', 'f48'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/extensions/objects',
                component: ComponentCreator('/garnet/docs/extensions/objects', '4dd'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/extensions/overview',
                component: ComponentCreator('/garnet/docs/extensions/overview', '821'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/extensions/procedure',
                component: ComponentCreator('/garnet/docs/extensions/procedure', 'ab6'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/extensions/raw-strings',
                component: ComponentCreator('/garnet/docs/extensions/raw-strings', '7e6'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/extensions/transactions',
                component: ComponentCreator('/garnet/docs/extensions/transactions', 'a1b'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/getting-started',
                component: ComponentCreator('/garnet/docs/getting-started', 'de7'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/getting-started/compaction',
                component: ComponentCreator('/garnet/docs/getting-started/compaction', 'f45'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/getting-started/configuration',
                component: ComponentCreator('/garnet/docs/getting-started/configuration', '389'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/getting-started/memory',
                component: ComponentCreator('/garnet/docs/getting-started/memory', '3bb'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/research/papers',
                component: ComponentCreator('/garnet/docs/research/papers', '005'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/security',
                component: ComponentCreator('/garnet/docs/security', '4e6'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/welcome/about-us',
                component: ComponentCreator('/garnet/docs/welcome/about-us', 'cf3'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/welcome/compatibility',
                component: ComponentCreator('/garnet/docs/welcome/compatibility', 'd6e'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/welcome/faq',
                component: ComponentCreator('/garnet/docs/welcome/faq', 'a57'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/welcome/features',
                component: ComponentCreator('/garnet/docs/welcome/features', '13a'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/welcome/news',
                component: ComponentCreator('/garnet/docs/welcome/news', '585'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/welcome/releases',
                component: ComponentCreator('/garnet/docs/welcome/releases', '7cc'),
                exact: true,
                sidebar: "garnetDocSidebar"
              },
              {
                path: '/garnet/docs/welcome/roadmap',
                component: ComponentCreator('/garnet/docs/welcome/roadmap', '845'),
                exact: true,
                sidebar: "garnetDocSidebar"
              }
            ]
          }
        ]
      }
    ]
  },
  {
    path: '/garnet/',
    component: ComponentCreator('/garnet/', '5b9'),
    exact: true
  },
  {
    path: '*',
    component: ComponentCreator('*'),
  },
];
