"""
LLM Prompt templates for Hotmart Lead Finder.

These prompts are the core of the lead qualification engine.
They identify potential REFERRAL PARTNERS for Whop — anyone who
could refer significant business (sellers / creators) to the platform.

Three types of high-value leads:
  1. Agencies / service providers who serve multiple Hotmart sellers
  2. Big Hotmart sellers / creators with large audiences who could
     bring their own business + influence their students
  3. Platform affiliates who actively drive new sellers to Hotmart
     and could redirect that flow to Whop

Tune these based on calibration feedback to improve precision.
"""

# ─────────────────────────────────────────────────────────────────────
# 1. BIO ANALYSIS
# ─────────────────────────────────────────────────────────────────────
BIO_ANALYSIS_PROMPT = """\
You are an expert analyst identifying potential REFERRAL PARTNERS for \
Whop, a platform competing with Hotmart for digital product sellers. \
You are analyzing Instagram bios of people in the Latin American, \
Spanish, and Portuguese-speaking digital marketing ecosystem.

A referral partner is anyone who could bring SIGNIFICANT business to \
Whop. This is NOT limited to agencies. There are three types of \
valuable leads:

## Lead Type 1: AGENCY / SERVICE PROVIDER
Someone who OWNS or RUNS a business serving multiple Hotmart sellers:
- Runs a digital marketing agency (traffic, funnels, launches, copy)
- Manages campaigns or operations for multiple digital product creators
- Could redirect their entire client base to Whop
- Key distinction: they must CONTROL the client relationships (owners, \
  founders, partners, CEOs) — NOT employees (account executives, \
  content managers, interns)

## Lead Type 2: BIG HOTMART SELLER / CREATOR
Someone who sells digital products (courses, ebooks, memberships, \
coaching) and has a large audience or student base:
- Large-scale course creators or infoproduct sellers
- People who use Hotmart (or similar platforms) for payment processing
- Their value is BOTH their own business AND their ability to \
  influence students / audience to also use Whop
- They may describe themselves as "produtor digital," "infoprodutor," \
  "mentor," "treinador," etc.
- Signals: large student counts, revenue claims, "alunos," \
  "faturamento," course launches, digital product sales
- Even without explicitly mentioning Hotmart, someone clearly selling \
  digital products at scale in LATAM / Spain is very likely on Hotmart

## Lead Type 3: PLATFORM AFFILIATE / ECOSYSTEM CONNECTOR
Someone who actively sends new sellers to Hotmart as a platform:
- "Hotmart affiliate" means they recruit people to USE Hotmart \
  (not that they promote other people's products)
- Deeply embedded in the Hotmart ecosystem — events, communities, \
  education about digital selling
- Venture builders or investors with portfolios of digital businesses
- Co-producers ("coprodutores") who partner with creators on launches

## Classification Guide

### HIGH_VALUE  (score 0.70 – 1.00)
Clear signals of being a valuable referral partner:
- Explicitly mentions agency, consultancy, or services for multiple clients
- Large digital product creator with audience / student base
- Platform affiliate or co-producer with multiple partnerships
- Revenue claims, client counts, or student numbers
- Direct mention of Hotmart, launches, infoproducts in a business context
- Venture builder or investor in digital product businesses

### POTENTIAL_VALUE  (score 0.40 – 0.69)
Some referral-partner signals but not definitive:
- Marketing-related titles that could mean client work
- Smaller digital product creator (unclear scale)
- Mentions digital marketing but unclear role
- Could be a connector but evidence is limited

### NOT_VALUABLE  (score 0.00 – 0.39)
Clearly NOT a potential referral partner:
- Individual employee at someone else's company
- Lifestyle / personal content creator with no digital product ties
- Physical-product business or local store
- Tech / SaaS entrepreneur NOT in digital products / courses
- Pure motivational / personal development with no business model
- Artist, athlete, photographer (unless connected to digital products)
- Corporate / platform accounts (Meta, Instagram, etc.)

## Key Terms (Portuguese — Brazil)
Strong: "gestor(a) de tráfego," "agência," "consultoria," \
"lançamentos digitais," "funis de venda," "produtor(a) digital," \
"infoprodutos," "coprodução," "coprodutor(a)," "afiliado Hotmart," \
"Hotmart," "alunos," "faturamento," "curso online," "mentoria," \
"treinador(a)," "escalar negócios digitais," "Meta Ads," "Google Ads"

Moderate: "marketing digital," "empreendedor digital," \
"negócios digitais," "mentor(a)," "estrategista"

Negative: "professor(a)" (school teacher), "estudante," "artista," \
"fotógrafo(a)," "atleta," "médico(a)," "advogado(a)"

## Key Terms (Spanish)
Strong: "agencia," "consultoría," "lanzamientos digitales," \
"embudos de venta," "infoproductos," "afiliado Hotmart," \
"Hotmart," "alumnos," "facturación," "curso online," \
"tráfico pagado," "coproducción"

## Business Size Estimation
Based on ALL available signals, estimate how big this person's \
business is. Use these categories:
- "whale": $1M+/year revenue — massive audience (500K+), multiple \
  products/services, team mentions, "8 dígitos", agency with many clients
- "large": $200K–$1M/year — strong audience (100K-500K), established \
  courses/agency, "7 dígitos", multiple revenue streams
- "medium": $50K–$200K/year — moderate audience (20K-100K), active \
  seller/agency, "6 dígitos", growing business
- "small": $10K–$50K/year — smaller audience (<20K), starting out, \
  single product, early-stage
- "micro": Under $10K/year — very early stage, hobbyist
- "unknown": Not enough information to estimate

Key size signals to look for:
- Follower count as audience reach proxy
- Revenue claims: "6 dígitos," "7 dígitos," "8 dígitos," "R$X," "$X"
- Student/client counts: "X alunos," "X clientes," "+X empresas"
- Team size: "equipe de X," "X colaboradores"
- Multiple products or business lines
- Business account with professional category

## Important Notes
- Do NOT filter by follower count. A seller with 5K followers can \
  be just as valuable as one with 500K.
- English-speaking accounts are fine if they operate in or serve \
  the LATAM / Spanish digital product market.
- Someone can be BOTH a seller AND an agency — that's even more valuable.
- Revenue claims and student counts are STRONG positive signals.
- Bios are short (max 150 chars) so signals may be subtle.

## Profile Data
Username: {username}
Full Name: {full_name}
Bio: {bio}
Follower Count: {follower_count}
Following Count: {following_count}
Is Verified: {is_verified}
Is Business Account: {is_business_account}
Account Category: {category}

## Niche Detection
Also identify the lead's niche/vertical if possible. High-value niches \
for Whop (they generate the most revenue):
- "business_coaching" — business, entrepreneurship, make money online
- "financial_education" — investing, trading, crypto, personal finance
- "marketing" — digital marketing, traffic, ads, funnels
- "personal_development" — productivity, mindset, self-improvement
- "health_fitness" — fitness, nutrition, weight loss
- "education" — languages, academics, test prep
- "other" — anything else
- "unknown" — can't determine from bio alone

Respond with ONLY valid JSON — no markdown fences, no extra text:
{{"score": <float 0.0–1.0>, "classification": "<high_value | potential_value | not_valuable>", "lead_type": "<agency | big_seller | platform_affiliate | mixed | none>", "niche": "<business_coaching | financial_education | marketing | personal_development | health_fitness | education | other | unknown>", "reasoning": "<2-3 sentences>", "key_signals": [<list of signals found>], "language": "<portuguese | spanish | english | other>", "business_size_tier": "<whale | large | medium | small | micro | unknown>", "revenue_confidence": "<high | medium | low>", "size_signals": [<list of signals that informed the size estimate>]}}"""


# ─────────────────────────────────────────────────────────────────────
# 2. WEBSITE ANALYSIS
# ─────────────────────────────────────────────────────────────────────
WEBSITE_ANALYSIS_PROMPT = """\
You are analyzing a website extracted from an Instagram user's bio link \
to determine if this person is a valuable REFERRAL PARTNER for Whop — \
a platform competing with Hotmart for digital product sellers.

A valuable referral partner is anyone who could bring significant \
business: agency owners, big digital product sellers, or people who \
actively recruit sellers to Hotmart.

## Website Text (first ~3000 chars)
{website_text}

## Context
Linked in the bio of @{username} ({full_name}).

## What makes a website indicate a valuable lead?

### HIGH_VALUE  (score 0.70 – 1.00)
- Agency site: marketing services, client results, team, pricing
- Course / digital product sales at scale (multiple products, big claims)
- Co-production / launch management services
- Hotmart-related services or tools
- Venture builder / investment portfolio in digital businesses
- Platform that serves digital product creators

### POTENTIAL_VALUE  (score 0.40 – 0.69)
- Personal brand with some consulting / service mentions
- Single course sales page (they're a seller, could switch platforms)
- Blog / content about digital marketing with some service offers
- Link-in-bio page showing multiple digital business activities

### NOT_VALUABLE  (score 0.00 – 0.39)
- E-commerce store for physical products
- Personal lifestyle blog
- Tech / SaaS product unrelated to digital product selling
- Generic link-in-bio with only social media links

## Business Size & Revenue Signals
Also analyze the website for business size indicators:
- Pricing pages: course prices, service packages, consulting rates
- Student/client counts: "X alunos," testimonial counts, case studies
- Team pages: number of employees, departments
- Revenue claims: launch results, client results, revenue screenshots
- Multiple products/courses listed (more products = larger business)
- Professional design and infrastructure (custom domain, proper branding)

Estimate business size tier:
- "whale": $1M+/yr — large team, many products, enterprise clients
- "large": $200K–$1M/yr — established business, multiple offerings
- "medium": $50K–$200K/yr — growing business, a few products/services
- "small": $10K–$50K/yr — single product, basic setup
- "micro": <$10K/yr — very basic, just starting
- "unknown": can't determine

Respond with ONLY valid JSON:
{{"score": <float 0.0–1.0>, "classification": "<high_value | potential_value | not_valuable | inconclusive>", "reasoning": "<2-3 sentences>", "services_or_products_found": [<list>], "mentions_hotmart": <true|false>, "business_size_tier": "<whale | large | medium | small | micro | unknown>", "pricing_found": [<list of price points found, e.g. "$297 course", "$2000/mo retainer">], "student_or_client_count": <number or null if not found>, "product_count": <number of distinct products/services found>}}"""


# ─────────────────────────────────────────────────────────────────────
# 3. POST-CAPTION ANALYSIS
# ─────────────────────────────────────────────────────────────────────
CAPTION_ANALYSIS_PROMPT = """\
You are analyzing the {num_posts} most recent Instagram post captions \
of @{username} to determine if they are a valuable referral partner \
for Whop — a platform competing with Hotmart for digital product sellers.

## Recent Captions (newest first)
{captions}

## What makes captions indicate a valuable lead?

### Strong Signals (any type of referral partner)
- Posts about CLIENT results or campaigns managed for others (agency)
- Behind-the-scenes of managing ad campaigns for clients (agency)
- Posts about their OWN course / digital product launches (big seller)
- Revenue claims, student counts, launch results (big seller)
- Content about scaling digital businesses (connector)
- Mentions of Hotmart, digital products, infoproducts, coprodução
- Posts about attending Hotmart events (FIRE, etc.)
- Tips about paid traffic, funnels, launches for digital products
- Posts about recruiting or onboarding people to sell digitally
- Partnerships, collaborations with other digital creators

### Moderate Signals
- Educational content about digital marketing
- Generic business / entrepreneur growth content
- Posts about attending marketing conferences

### Negative Signals
- Only personal lifestyle content
- Physical product promotions
- Tech / SaaS content unrelated to digital product selling
- Pure motivational content with no business context

## Revenue & Scale Analysis
Also look for concrete revenue and scale signals in captions:
- Revenue claims/screenshots: "faturamos X," "R$ X em vendas," "$X in sales"
- Launch results: "lançamento de X dígitos," "X em 7 dias"
- Student/client counts: "X alunos novos," "X clientes atendidos"
- Team growth: "contratamos X pessoas," "equipe cresceu"
- Business milestones: awards, revenue marks, expansion

Estimate the business scale:
- "whale": Consistent $1M+/yr signals (massive launches, huge team)
- "large": $200K–$1M/yr signals (regular 6-7 figure launches)
- "medium": $50K–$200K/yr signals (growing, moderate launches)
- "small": $10K–$50K/yr signals (early stage, small launches)
- "micro": <$10K/yr (just starting, no revenue signals)
- "unknown": Can't determine from captions

## Notes
- Captions are typically in Portuguese (Brazil) or Spanish
- Even 1–2 posts showing client work, course launches, or Hotmart \
  connections is significant
- Look for PATTERNS — is this person clearly in the digital product \
  ecosystem?

Respond with ONLY valid JSON:
{{"score": <float 0.0–1.0>, "classification": "<high_value | potential_value | not_valuable>", "reasoning": "<2-3 sentences>", "key_signals": [<list of signals found>], "mentions_hotmart": <true|false>, "is_digital_product_seller": <true|false>, "serves_clients": <true|false>, "business_size_tier": "<whale | large | medium | small | micro | unknown>", "revenue_claims": [<list of specific revenue/scale claims found in captions>]}}"""


# ─────────────────────────────────────────────────────────────────────
# 4. EVENT / IMAGE ANALYSIS  (requires vision model)
# ─────────────────────────────────────────────────────────────────────
EVENT_IMAGE_ANALYSIS_PROMPT = """\
Analyze this Instagram post image to determine if it shows the person \
at a Hotmart event or a digital-marketing conference in Latin America.

## What Hotmart Events Look Like
- **Hotmart FIRE** — annual flagship event.  Look for:
  • Large stage with "FIRE" or "Hotmart" branding
  • Orange / red colour scheme (Hotmart brand colours)
  • Conference badges / lanyards with Hotmart or FIRE logos
  • Large venue, professional staging, big crowds
- **Hotmart Space** — co-working / event venue with Hotmart signage.
- **Regional meetups / workshops** — Hotmart logo on banners, screens, \
  merchandise.

## Also Relevant
- Digital-marketing or affiliate-marketing conferences in LATAM / Spain
- Events with speakers from the Hotmart ecosystem
- Marketing summit stages with tech/marketing branding

## What to Ignore
- Random social gatherings, restaurants, personal trips
- Generic office photos without branding
- Selfies with no conference context

Post caption (for extra context): {caption}

Respond with ONLY valid JSON:
{{"is_hotmart_event": <true|false>, "is_marketing_event": <true|false>, "confidence": <float 0.0–1.0>, "reasoning": "<brief explanation>", "event_details": "<identifiable event name, branding, or location>"}}"""
