# Java EE版COC框架

### 自定义路由
	Router.get("frontend", "/news/{date}/{id}.shtml", new Generator() {
		@Override
		public RouteAction call(String... args) {
			Map<String, String[]> params = new Hashtable<>();
			params.put("date", new String[] {args[0]});
			params.put("id", new String[] {args[1]});
			return new RouteAction("index", "news", params);
		}
	});

### JDBC链式操作

	demoDao.where("status=1").orderBy("id desc").limit(30).all();

### 更多特性
> 
- 约定路由（Controller/Action）
- 命名参数（NamedParameter）
- 采用Gradle管理项目
- 支持多项目多域名
- 数据库连接池及主从切换