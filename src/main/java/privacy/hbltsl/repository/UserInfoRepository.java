package privacy.hbltsl.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import privacy.hbltsl.model.UserInfo;

@Repository
public class UserInfoRepository {

	@Resource
	JdbcTemplate jdbcTemplate;

	public void replace(final List<UserInfo> list) throws Exception {

		jdbcTemplate.execute(new ConnectionCallback<List<UserInfo>>() {

			String sql = "insert into user_info ( " //
					+ " name, email, create_time  " // 1
					+ " ) values(?, ?, ?)" // 1
					;
			Long id = 0L;

			@Override
			public List<UserInfo> doInConnection(Connection c) throws SQLException, DataAccessException {
				System.out.println("<< Connection=" + c.hashCode() + " list=" + list.hashCode() + " c="
						+ c.getTransactionIsolation());
				boolean autoCommit = c.getAutoCommit();
				c.setAutoCommit(false);
				PreparedStatement p = c.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
				int index = 0;
				for (UserInfo ui : list) {
					index = 1;
					// 1
					p.setString(index++, ui.getName());
					p.setString(index++, ui.getEmail());
					if (ui.getCreateTime() == null) {
						p.setNull(index++, Types.TIMESTAMP);
					} else {
						p.setTimestamp(index++, new Timestamp(ui.getCreateTime().getTime()));
					}

					p.addBatch();
				}

				int executeBatch[] = p.executeBatch();

				c.commit();

				System.out.println("=> Connection=" + c.hashCode() + " list=" + list.hashCode() + " size=" + list.size()
						+ " x=" + Arrays.toString(executeBatch));

				index = 0;
				ResultSet r = p.getGeneratedKeys();
				while (r.next()) {
					list.get(index++).setId(id = r.getLong(1));
					System.out.println(id);
				}
				p.close();
				c.setAutoCommit(autoCommit);

				System.out
						.println(">> Connection=" + c.hashCode() + " list=" + list.hashCode() + " size=" + list.size());

				return list;
			}
		});

	}

}
