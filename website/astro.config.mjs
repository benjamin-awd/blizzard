// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import d2 from 'astro-d2';

// https://astro.build/config
export default defineConfig({
	site: 'https://benjamin-awd.github.io',
	base: '/blizzard',
	integrations: [
		d2(),
		starlight({
			title: '❄️ Blizzard',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/benjamin-awd/blizzard' },
			],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Introduction', slug: 'index' },
					],
				},
				{
					label: '⚙️ Core Concepts',
					items: [
						{ label: 'Topology', slug: 'concepts/topology' },
						{ label: 'Watermarking', slug: 'concepts/watermarking' },
						{ label: 'Storage Backends', slug: 'concepts/storage' },
						{ label: 'Fault Tolerance', slug: 'concepts/fault-tolerance' },
					],
				},
				{
					label: '❄️ Blizzard',
					items: [
						{ label: 'Overview', slug: 'architecture/overview' },
						{ label: 'Source Processing', slug: 'architecture/source' },
						{ label: 'Parquet Writer', slug: 'architecture/sink' },
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'Dead Letter Queue', slug: 'reference/dlq' },
					],
				},
				{
					label: '🐧 Penguin',
					items: [
						{ label: 'Overview', slug: 'penguin' },
						{ label: 'Delta Lake Commits', slug: 'penguin/delta-lake' },
						{ label: 'Schema Evolution', slug: 'penguin/schema-evolution' },
						{ label: 'Configuration', slug: 'penguin/configuration' },
					],
				},
			],
			editLink: {
				baseUrl: 'https://github.com/benjamin-awd/blizzard/edit/main/website/',
			},
			head: [
				{
					tag: 'meta',
					attrs: {
						property: 'og:image',
						content: 'https://benjamin-awd.github.io/blizzard/og.png',
					},
				},
			],
		}),
	],
});
