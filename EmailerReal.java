package com.backend.email;

// real emailer - connects to an SMTP server and sends mail
// just streams results out to a file...
import java.io.*;
import java.util.*;

import javax.mail.*;
import javax.mail.internet.*;

import java.sql.*;
import com.backend.util.*;

public class EmailerReal extends Emailer
{
	static public final String _cvs_Id = "$Id: Emailer.java,v 1.0 2004/09/20 12:37:00 Dave Newington Exp $";
	static public final String _cvs_Name = "$Name: Backend-Email-REL-1_0_0 $";

	private String m_strSmtpHost;
	private String m_appName;

	private Vector m_messageQueue = new Vector();
	private Thread m_mailSender = null;
	private Session m_session;
	private boolean m_bMultiThread = false;         // if true, email is sent in a separate thread




	//mail.store.protocol, mail.transport.protocol, mail.host, mail.user, and mail.from
	EmailerReal(String appName)
	{
		m_appName = appName;
		m_strSmtpHost = "10.254.0.20";
		m_bMultiThread = true;
		Properties props = System.getProperties();
		m_session = Session.getInstance(props, null);
		m_session.setDebug(false);
	}

	static private String arrayToString(Address []array)
	{
		StringBuffer strTotalString = new StringBuffer();

		for (int i = 0; array != null && i < array.length; i++)
		{
			if(i>0)
				strTotalString.append("; ");
			strTotalString.append(array[i].toString());
		}
		return strTotalString.toString();
	}

	public void send(String from,
				String recipients,
				String ccs,
				String strSubject,
				String strBody
				) throws IOException
	{
		try
		{
			int nTime = (int)System.currentTimeMillis();
			int nKey  = (nTime >>> 8) | (nTime << 24);
			String insertSql = "INSERT INTO T_EMAIL_MSG (EmailKey,from,recipients,ccs,strSubject,strBody)"
						   + " VALUES (?, ?, ?, ?, ?, ?) ";
			Connection conn = null;
			PreparedStatement insertStatement = null;
//			System.out.println("Email Key  " + nKey);
//			System.out.println("Email from " + from);
//			System.out.println("Email recipients " + recipients);
//			System.out.println("Email ccs " + ccs);
//			System.out.println("Email strSubject " + strSubject);
//			System.out.println("Email strBody " + strBody);

            PoolManager pool = PoolManagerFactory.getInstnace().getPoolManager();
			try 
			{
			   conn = pool.getConnection();
			   insertStatement = conn.prepareStatement(insertSql);
			   insertStatement.setInt(1,nKey);
			   if (from == null)
			      insertStatement.setString(2,"");
			   else   
			      insertStatement.setString(2,from);
			   insertStatement.setString(3,recipients);
			   if (ccs == null)
			      insertStatement.setString(4,"");
			   else	
			      insertStatement.setString(4,ccs);
			   insertStatement.setString(5,strSubject);
			   insertStatement.setString(6,strBody);
			   insertStatement.executeUpdate();
			}
			catch (SQLException sq)
			{
				System.out.println("Email DB Send() #0 for " + nKey + " " + sq.getMessage());
			}
			finally
			{
			   insertStatement.close();
			   pool.closeConnection(conn);
			}   


			if (!m_bMultiThread)
			{
			    System.out.println("Email to: " + recipients + (ccs!=null?(","+ccs):""));
			    // Construct a message
			    MimeMessage message = new MimeMessage(m_session);
			    message.setFrom(new InternetAddress(from));
			    message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients));
			    if (ccs!=null)
				    message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(ccs));
			    message.setSubject(strSubject);
			    message.setText(strBody, "ISO-8859-1");
				sendImmediate(message,nKey);
			}
		}
		catch (Exception e)
		{
			System.out.println(e + "EmailerReal.send()");
			throw new IOException(e.getMessage());
		}
	}


	/*************************************************************************
	 * Read the DB2 table and send any outstanding Emails.
	 * Method is here to allow for dead thread
	 */
	public synchronized void ProcessQueue()
	{
	   Transport transport = null;
//	   System.out.println("Processing Queue");
	   Connection conn = null;
	   PoolManager pool = PoolManagerFactory.getInstnace().getPoolManager();
	   try {

		    conn = pool.getConnection();
	        conn.setAutoCommit(false);
		    PreparedStatement statement = null;
		    PreparedStatement updateStatement = null;
		    PreparedStatement errorStatement = null;
	        String from = null;
	        String recipients = null;
	        String ccs = null;
	        String strSubject = null;
	        String strBody = null;
		
			try {

				String selectSql = "SELECT * FROM T_EMAIL_MSG WHERE (Processed = 'N') with CS ";
	  	    	String insertSql = "UPDATE T_EMAIL_MSG set processed = 'Y'"
					       + " where EmailKey = ? ";
		    	statement = conn.prepareStatement(selectSql);
		    	updateStatement = conn.prepareStatement(insertSql);

				ResultSet rs = statement.executeQuery();
				transport = m_session.getTransport("smtp");
				transport.connect(m_strSmtpHost, "", "");
//		   		System.out.println("Connected to transport");
				rs.next(); 
				do
				{

					int nKey = rs.getInt("EmailKey");

					from = "" + rs.getString("from");
					recipients = "" + rs.getString("recipients");
					ccs = "" + rs.getString("ccs");
					strSubject = "" + rs.getString("strSubject");
					strBody = "" + rs.getString("strBody");

			    	String to = "";
					try
					{
//						System.out.println("Email to: " + recipients + (ccs!=null?(","+ccs):""));
	  		    		// Construct a message
	    				MimeMessage message = new MimeMessage(m_session);
	    				message.setFrom(new InternetAddress(from));
	    				message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients));
	    				if (ccs!=null)
	    				{
	 	    				message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(ccs));
	    				}	
	    				message.setSubject(strSubject);
	    				message.setText(strBody, "ISO-8859-1");
						to = arrayToString(message.getAllRecipients());
//						System.out.println("About to send message");
                        if (from.equals(recipients) && strSubject.equals(""))
                        {
//							System.out.println("Send the message");
                        }
						else
						{
							transport.sendMessage(message, message.getAllRecipients());
						}
//						System.out.println("Message " + nKey + " was successfully sent to " + to);
						try
						{
				       		updateStatement.setInt(1,nKey);
				       		updateStatement.executeUpdate();
		  				}
		    			catch (SQLException sq)
		    			{
			    			System.out.println("EmailerReal.senderThread DB Update() #0 "+ nKey + " : " + sq.getMessage());
		    			}
						finally
						{
							conn.commit();
						}
		    		} 
					catch(Exception e1)
					{
						System.out.println(e1 + "EmailerReal.senderThread " +
						"error sending mail to " + to + ": " + e1.getMessage());
	  	    			String errorSql = "UPDATE T_EMAIL_MSG set processed = 'X', exception = ?"
					       + " where EmailKey = ? ";
		    			errorStatement = conn.prepareStatement(errorSql);
						try
						{
				       		errorStatement.setString(1,"" + e1);
				       		errorStatement.setInt(2,nKey);
				       		errorStatement.executeUpdate();
		  				}
		    			catch (SQLException sq)
		    			{
			    			System.out.println("EmailerReal.senderThread DB Error() #0 "+ nKey + " : " + sq.getMessage());
		    			}
						finally
						{
							if (errorStatement != null) 
							   errorStatement.close();
						 	conn.commit();
						}
					} 
			   } while (rs.next());
			   transport.close();
			}
			catch (Exception e2)
			{
				System.out.println("EmailerReal.senderThread " + e2.getMessage());
			}
			finally
			{
				if (updateStatement != null) 
  				   updateStatement.close();
				if (statement != null) 
				   statement.close();
			}
	   }
	   catch (Exception e3)
	   {
		   System.out.println("EmailerReal.senderThread " + e3.getMessage());
	   }
	   finally
	   {
		  try
		  { 
			conn.commit();
			conn.setAutoCommit(true);
		  }
		  catch (Exception e4)
		  {
		    System.out.println("EmailerReal.senderThread " + e4.getMessage());
	      }
		  finally
		  {	
	   	    pool.closeConnection(conn);
		  }  
	   }
//	   System.out.println("Ending sender thread");
	   m_mailSender = null;
	}
	/*************************************************************************
	 * Read the DB2 table and return count of outstanding mails.
	 */
	public int QueueDepth()
	{
	   Connection conn = null;
	   int  qDepth = 0;
	   PoolManager pool = PoolManagerFactory.getInstnace().getPoolManager();
	   try {

		conn = pool.getConnection();
		PreparedStatement statement = null;
			
		try 
		{

			String selectSql = "SELECT count(*) FROM T_EMAIL_MSG WHERE (Processed = 'N') with UR ";
			statement = conn.prepareStatement(selectSql);
			ResultSet rs = statement.executeQuery();

			if (rs.next()) 
			{

				qDepth = rs.getInt(1);
			}
		}	
		catch (Exception sq1)
		{
			System.out.println("Email Queue Depth #0" + sq1.getMessage());
		}
		finally
		{
			statement.close();
		}
	   }
	   catch (Exception sq2)
	   {
		System.out.println("Email Queue Depth #1 " + sq2.getMessage());
	   }
	   finally
	   {
	      pool.closeConnection(conn);
	   }
	   return qDepth;
	}
	/**************************************************************************
	 * Send the message immediately - in this thread
	 * Update Email_Log if message sent successfully.
	 */
	private void sendImmediate(MimeMessage message, int EmailKey)
	{
		String strTo = null;
		Transport transport = null;
		try
		{
		    Connection conn = null;
		    PreparedStatement updateStatement = null;
		    PreparedStatement errorStatement = null;
			PoolManager pool = PoolManagerFactory.getInstnace().getPoolManager();
			try
			{
			    strTo = arrayToString(message.getAllRecipients());
			    transport = m_session.getTransport("smtp");
			    transport.connect(m_strSmtpHost, "", "");
//			   	System.out.println("Connected to transport");
			    transport.sendMessage(message, message.getAllRecipients());
//			   	System.out.println("Message was successfully sent to " + strTo);
	  		    String insertSql = "UPDATE T_EMAIL_MSG set processed = 'Y'"
						       + " where EmailKey = ? ";
 		        conn = pool.getConnection();

 			    try 
			    {
			       updateStatement = conn.prepareStatement(insertSql);
			       updateStatement.setInt(1,EmailKey);
			       updateStatement.executeUpdate();
			    }
			    catch (SQLException sq)
			    {
				    System.out.println("Email DB Immediate() #0 " + sq.getMessage());
			    }
			    finally
			    {
			       updateStatement.close();
			       pool.closeConnection(conn);
			    }
			}
			catch(Exception e)
			{
				System.out.println("EmailerReal.senderThread " +
					"error sending mail " + EmailKey + " to " + strTo + ": " + e.getMessage());
				String errorSql = "UPDATE T_EMAIL_MSG set processed = 'X', Exception = ?"
				       + " where EmailKey = ? ";
				try
				{
			       	errorStatement = conn.prepareStatement(errorSql);
		       		errorStatement.setString(1,"" + e);
		       		errorStatement.setInt(2,EmailKey);
		       		errorStatement.executeUpdate();
  				}
				catch (SQLException sq)
				{
	    			System.out.println("EmailerReal.senderThread DB Error() #0 "+ EmailKey + " : " + sq.getMessage());
				}
				finally
				{
			  	    errorStatement.close();
					conn.commit();
				}
			}
			finally
			{
				if (transport != null)
				{
					try
					{
						transport.close();
					}
					catch (Exception e)
					{
						System.out.println(e + "EmailerReal - error closing connection");
					}
				}
			}
		}
		catch (Exception e)
		{
			System.out.println(e + "EmailerReal.senderThread " +
				"error sending mail " + EmailKey + " to " + strTo + ": " + e.getMessage());
		}
	}
	public String RunMode()
	{
		if(m_bMultiThread)
		   return "Async";
		else
		   return "Sync";
	}
}