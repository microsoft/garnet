/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  // By default, Docusaurus generates a sidebar from the docs folder structure
  tutorialSidebar: [{type: 'autogenerated', dirName: '.'}],

	garnetDocSidebar: [
		{type: 'category', label: 'Welcome', collapsed: false, items: ["welcome/intro", "welcome/features", "welcome/releases", "welcome/compatibility", "welcome/roadmap", "welcome/faq", "welcome/about-us"]},
		{type: 'category', label: 'Getting Started', items: ["getting-started/build", "getting-started/configuration", "getting-started/security"]},
		{type: 'category', label: 'Benchmarking', items: ["benchmarking/overview", "benchmarking/results-resp-bench", "benchmarking/resp-bench"]},
		{type: 'category', label: 'Commands', items: ["commands/overview", "commands/api-compatibility", "commands/raw-string", "commands/generic-commands", "commands/analytics-commands", "commands/data-structures", "commands/server-commands", "commands/checkpoint-commands", "commands/transactions-commands", "commands/cluster"]},
		{type: 'category', label: 'Server Extensions', items: ["extensions/transactions", "extensions/raw-strings", "extensions/objects"]},
		{type: 'category', label: 'Cluster Mode', items: ["cluster/overview", "cluster/replication", "cluster/key-migration"]},
		{type: 'category', label: 'Developer Guide', items: ["dev/onboarding", "dev/code-structure", "dev/configuration", "dev/network", "dev/processing", "dev/garnet-api",
		  {type: 'category', label: 'Tsavorite - Storage Layer', collapsed: false, items: ["dev/tsavorite/intro", "dev/tsavorite/reviv", "dev/tsavorite/locking"]},
		  "dev/transactions",
		  "dev/custom-commands",
		  "dev/cluster",
		  "dev/contributing"]},
		/*
		{type: 'category', label: 'Command Reference', items: ["commands", "pubsub", "transactions"]},
		{type: 'category', label: 'Configuration', collapsed: true, items: ["storage", "compaction", "config-file"]},
		{type: 'category', label: 'Docker', items: [
			{type: 'link', label: 'Advanced', href: 'https://hub.docker.com/r/microsoft/Garnet'},
			"dockerfiles"]},
		{type: 'category', label: 'Security', items: ["tls", "acl"]},
		{type: 'category', label: 'Diagnostics', items: ["logger", "metrics"]},
		"research",
			*/
		{type: 'category', label: 'Other Links', items: [
			{type: 'link', label: 'Releases', href: 'https://github.com/microsoft/garnet/releases'},
                        {type: 'link', label: 'License', href: 'https://github.com/microsoft/garnet/blob/main/LICENSE'},
                        {type: 'link', label: 'Privacy', href: 'https://privacy.microsoft.com/en-us/privacystatement'},
			]}
	],

  // But you can create a sidebar manually
  /*
  tutorialSidebar: [
    'intro',
    'hello',
    {
      type: 'category',
      label: 'Tutorial',
      items: ['tutorial-basics/create-a-document'],
    },
  ],
   */
};

export default sidebars;
