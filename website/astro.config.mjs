// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://blizzard.dev',
	integrations: [
		starlight({
			title: 'Blizzard',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/vectordotdev/blizzard' },
			],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Introduction', slug: 'index' },
					],
				},
				{
					label: 'Architecture',
					items: [
						{ label: 'Pipeline Overview', slug: 'architecture/pipeline' },
						{ label: 'Checkpoint & Recovery', slug: 'architecture/checkpoint-recovery' },
						{ label: 'Storage Backends', slug: 'architecture/storage' },
						{ label: 'Source Processing', slug: 'architecture/source' },
						{ label: 'Sink & Delta Lake', slug: 'architecture/sink' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'Error Handling', slug: 'reference/errors' },
						{ label: 'Dead Letter Queue', slug: 'reference/dlq' },
						{ label: 'Metrics', slug: 'reference/metrics' },
					],
				},
			],
			editLink: {
				baseUrl: 'https://github.com/vectordotdev/blizzard/edit/main/website/',
			},
			head: [
				{
					tag: 'meta',
					attrs: {
						property: 'og:image',
						content: 'https://blizzard.dev/og.png',
					},
				},
			],
		}),
	],
});
