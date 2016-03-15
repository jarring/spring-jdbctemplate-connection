package privacy.hbltsl.test;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import privacy.hbltsl.model.UserInfo;
import privacy.hbltsl.repository.UserInfoRepository;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:ApplicationContext.xml" })
public class CustomUnitTest {

	@Resource
	UserInfoRepository userInfoRepository;

	@Test
	public void test() throws Exception {
		List<UserInfo> list = new ArrayList<UserInfo>();
		for (int i = 1; i <= 10; i++) {
			UserInfo ui = new UserInfo();
			ui.setName("name-" + i);
			ui.setEmail("email-" + i);
			list.add(ui);
		}
		userInfoRepository.replace(list);

	}

}
